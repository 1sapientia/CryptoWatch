package orderbooks

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
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

type RequestItem struct {
	MarketID  int         `dynamodbav:"m"`
	Timestamp float64     `dynamodbav:"t"` //timestamp (ns)
	Items     [][]float64 `dynamodbav:"i"` // list of items where item is [timestamp, price, amount]
}

type Item struct {
	Timestamp float64 //market id
	Price     float64 //timestamp (ms)
	Amount    float64 //amount (positive bid, negative ask)
}

type DatabaseWriter struct {
	Session            *session.Session
	Client             *dynamodb.DynamoDB
	MarketDescriptor   *rest.MarketDescr
	itemCounter        map[string]int
	orderbookTableName string
	tradesTableName    string
	deltasQueue        []Item
	tradesQueue        []Item
}

func NewDatabaseWriter(marketDescriptor *rest.MarketDescr, orderbookTableName string, tradesTableName string) *DatabaseWriter {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// and region from the shared configuration file ~/.aws/config.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	// Create DynamoDB client
	cli := dynamodb.New(sess)
	dbw := &DatabaseWriter{
		Session:            sess,
		Client:             cli,
		MarketDescriptor:   marketDescriptor,
		orderbookTableName: orderbookTableName,
		tradesTableName:    tradesTableName,
		itemCounter: map[string]int{
			orderbookTableName: 0,
			tradesTableName:    0,
		},
		deltasQueue: []Item{},
		tradesQueue: []Item{},
	}
	return dbw
}

// writeCheckpoint writes the checkpoint item to the database
func (dbw *DatabaseWriter) writeCheckpoint() {
	items := []Item{{
		Timestamp: float64(time.Now().UnixNano()),
		Price:     0, // price zero indicates the checkpoint
		Amount:    0,
	}}
	fmt.Println(
		"writing checkpoint", dbw.MarketDescriptor,
		", orderbook items:", dbw.itemCounter[dbw.orderbookTableName],
		", trades items:", dbw.itemCounter[dbw.tradesTableName],
		time.Now())
	dbw.submitDeltas(items)
}

// writeDelta serializes the OrderBookDelta update and concurrently writes it to the orderbooks table
func (dbw *DatabaseWriter) writeDelta(obd common.OrderBookDelta) {
	items := dbw.extractDeltas(obd)
	dbw.submitDeltas(items)
}

// writeDelta transforms serializes the TradesUpdate and concurrently writes it to the trades table
func (dbw *DatabaseWriter) writeTrades(tu common.TradesUpdate) {
	items := dbw.extractTrades(tu)
	dbw.submitTrades(items)
}

// submitDeltas appends the deltas items to the queue which is then potentially sent to the database
func (dbw *DatabaseWriter) submitDeltas(items []Item) {
	dbw.deltasQueue = append(dbw.deltasQueue, items...)
	if len(dbw.deltasQueue) > 1000 {
		putItem := dbw.generatePutItem(dbw.deltasQueue[:1000], dbw.orderbookTableName)
		go dbw.writeWithExponentialBackoff(putItem, dbw.orderbookTableName)
		dbw.deltasQueue = dbw.deltasQueue[1000:]
	}
}

// submitTrades appends the trades items to the queue which is then potentially sent to the database
func (dbw *DatabaseWriter) submitTrades(items []Item) {
	dbw.tradesQueue = append(dbw.tradesQueue, items...)
	if len(dbw.tradesQueue) > 10 {
		putItem := dbw.generatePutItem(dbw.tradesQueue[:10], dbw.tradesTableName)
		go dbw.writeWithExponentialBackoff(putItem, dbw.tradesTableName)
		dbw.tradesQueue = dbw.tradesQueue[10:]
	}
}

// writeWithExponentialBackoff dispatches the put item with retries
// The AWS SDKs for DynamoDB automatically retries throttled requests unless the request queue is full
// (check ErrCodeProvisionedThroughputExceededException)
func (dbw *DatabaseWriter) writeWithExponentialBackoff(item *dynamodb.PutItemInput, tableName string) {
	numOfRetries := 10
	for i := 0; i < numOfRetries; i++ {
		_, err := dbw.Client.PutItem(item)
		if err == nil {
			return
		} else if aerr, ok := err.(awserr.Error); ok && aerr.Code() != dynamodb.ErrCodeProvisionedThroughputExceededException {
			fmt.Println("put item failed", err)
			return
		}
		fmt.Println("put item throttled with error. retry pending", err)

		delay := math.Pow(2.0, float64(i))
		log.Print("retrying in", delay, "seconds")
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
			Timestamp: float64(time.Now().UnixNano()),
			Amount:    amount,
			Price:     price,
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
			Timestamp: float64(time.Now().UnixNano()),
			Price:     price,
			Amount:    amount,
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
			Timestamp: float64(time.Now().UnixNano()),
			Price:     price,
			Amount:    amount,
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

// generatePutItem maps a list of items to a list of dynamodb write requests
func (dbw *DatabaseWriter) generatePutItem(items []Item, tableName string) *dynamodb.PutItemInput {

	timestamp := items[0].Timestamp
	itemslist := [][]float64{}
	for _, item := range items {
		itemslist = append(itemslist, []float64{item.Timestamp, item.Price, item.Amount})
	}

	requestItem := RequestItem{
		MarketID:  dbw.MarketDescriptor.ID,
		Timestamp: timestamp,
		Items:     itemslist,
	}

	av, err := dynamodbattribute.MarshalMap(requestItem)
	if err != nil {
		fmt.Println("Got error marshalling the put item:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}

	return input

}
