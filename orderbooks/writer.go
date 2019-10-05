package orderbooks

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"log"
	"math"
	"os"
	"strconv"
	"time"
)

type Item struct {
	M int     //market id
	T int64   //timestamp (ms)
	A float64 //amount (positive bid, negative ask)
	P float64 //price
}

type DatabaseWriter struct {
	Session          *session.Session
	Client           *dynamodb.DynamoDB
	MarketDescriptor *rest.MarketDescr
}

func NewDatabaseWriter(marketDescriptor *rest.MarketDescr) *DatabaseWriter {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// and region from the shared configuration file ~/.aws/config.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	// Create DynamoDB client
	cli := dynamodb.New(sess)
	return &DatabaseWriter{
		Session:          sess,
		Client:           cli,
		MarketDescriptor: marketDescriptor,
	}
}

// writeCheckpoint writes the checkpoint item to the database
func (dbw *DatabaseWriter) writeCheckpoint() {
	items := []Item{{
		M: dbw.MarketDescriptor.ID,
		T: time.Now().UnixNano(),
		A: 0,
		P: 0, // price zero indicates the checkpoint
	}}
	requestItems := generateRequestItems(items)
	fmt.Println("writing checkpoint", dbw.MarketDescriptor, time.Now().Minute())
	go dbw.write(requestItems, "orderbooks")
}

// writeDelta serializes the OrderBookDelta update and concurrently writes it to the orderbooks table
func (dbw *DatabaseWriter) writeDelta(obd common.OrderBookDelta) {
	items := dbw.extractDeltas(obd)
	requestItems := generateRequestItems(items)
	go dbw.write(requestItems, "orderbooks")
}

// writeDelta transforms serializes the TradesUpdate and concurrently writes it to the trades table
func (dbw *DatabaseWriter) writeTrades(tu common.TradesUpdate) {
	items := dbw.extractTrades(tu)
	requestItems := generateRequestItems(items)
	go dbw.write(requestItems, "trades")
}

// write splits the request items to batches and dispatches them
func (dbw *DatabaseWriter) write(requestItems []*dynamodb.WriteRequest, tableName string) {
	const chunkSize = 25
	for i := 0; i < len(requestItems); i += chunkSize {
		end := i + chunkSize
		if end > len(requestItems) {
			end = len(requestItems)
		}
		batch := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{tableName: requestItems[i:end]},
		}
		dbw.exponentialBackoff(batch, 5, tableName)
	}
}

// exponentialBackoff dispatches the request batch with retries
// The AWS SDKs for DynamoDB automatically retries throttled requests unless the request queue is full
// (check ErrCodeProvisionedThroughputExceededException)
func (dbw *DatabaseWriter) exponentialBackoff(batch *dynamodb.BatchWriteItemInput, numOfRetries int, tableName string) {
	for i := 0; i < numOfRetries; i++ {
		output, err := dbw.Client.BatchWriteItem(batch)
		if err != nil{
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException{
				log.Print("batch send fully throttled with error. retry pending", err)
			} else {
				log.Print("batch send failed", err)
				break
			}
		}
		if output.UnprocessedItems != nil && len(output.UnprocessedItems) != 0 {
			batch.RequestItems = output.UnprocessedItems
		}
		delay := math.Pow(2.0, float64(i))
		log.Print("retrying in", delay, "seconds, remaining items:", len(batch.RequestItems[tableName]))
		time.Sleep(time.Duration(delay) * time.Second)
	}
}

// extractTrades serializes the TradesUpdate to a list of Items
func (dbw *DatabaseWriter) extractTrades(tu common.TradesUpdate) []Item {
	var trades []Item

	parseTrade := func(newTrade common.PublicTrade) {
		amount, err1 := strconv.ParseFloat(newTrade.Amount, 64)
		price, err2 := strconv.ParseFloat(newTrade.Price, 64)
		if err1 != nil || err2 != nil {
			log.Print("trade string to float conversion failed", err1, err2)
			return
		}
		trades = append(trades, Item{
			M: dbw.MarketDescriptor.ID,
			T: time.Now().UnixNano(),
			A: amount,
			P: price,
		})
	}
	for _, newTrade := range tu.Trades {
		parseTrade(newTrade)
	}
	return trades
}

// extractDeltas serializes the OrderBookDelta update to a list of Items
func (dbw *DatabaseWriter) extractDeltas(obd common.OrderBookDelta) []Item {
	var deltas []Item

	parseOrders := func(newOrder common.PublicOrder, isAsk bool) {
		amount, err1 := strconv.ParseFloat(newOrder.Amount, 64)
		price, err2 := strconv.ParseFloat(newOrder.Price, 64)
		if err1 != nil || err2 != nil {
			log.Print("delta string to float conversion failed", err1, err2)
			return
		}
		if isAsk {
			amount *= -1
		}
		deltas = append(deltas, Item{
			M: dbw.MarketDescriptor.ID,
			T: time.Now().UnixNano(),
			A: amount,
			P: price,
		})
	}

	parseRemovals := func(removePrice string) {
		amount := 0.0 // remove
		price, err2 := strconv.ParseFloat(removePrice, 64)
		if err2 != nil {
			log.Print("delta string to float conversion failed", err2)
			return
		}
		deltas = append(deltas, Item{
			M: 1001,
			T: time.Now().UnixNano(),
			A: amount,
			P: price,
		})
	}

	for _, newOrder := range obd.Asks.Set {
		parseOrders(newOrder, true)
	}
	for _, newOrder := range obd.Bids.Set {
		parseOrders(newOrder, false)
	}

	for _, removePrice := range obd.Asks.Remove {
		parseRemovals(removePrice)
	}
	for _, removePrice := range obd.Bids.Remove {
		parseRemovals(removePrice)
	}
	return deltas
}

// generateRequestItems maps a list of items to a list of dynamodb write requests
func generateRequestItems(items []Item) []*dynamodb.WriteRequest {
	writeRequests := make([]*dynamodb.WriteRequest, len(items))
	// transform deltas to write requests
	for i, delta := range items {
		av, err := dynamodbattribute.MarshalMap(delta)
		if err != nil {
			fmt.Println("Got error marshalling new movie item:")
			fmt.Println(err.Error())
			os.Exit(1)
		}
		writeRequests[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: av,
			},
		}
	}
	return writeRequests
}
