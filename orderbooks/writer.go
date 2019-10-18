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
	"github.com/gocql/gocql"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
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
	CassandraSession *gocql.Session
	CassandraCluster *gocql.ClusterConfig

	DynamoSession *session.Session
	Client        *dynamodb.DynamoDB

	MarketDescriptor   *rest.MarketDescr
	itemCounter        map[string]int
	orderbookTableName string
	tradesTableName    string
	deltasQueue        []Item
	tradesQueue        []Item
}

func NewDatabaseWriter(marketDescriptor *rest.MarketDescr, orderbookTableName string, tradesTableName string) *DatabaseWriter {

	cassandraCluster := gocql.NewCluster("127.0.0.1")
	cassandraCluster.Keyspace = "orderbookretriever"
	cassandraSession, err := cassandraCluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// and region from the shared configuration file ~/.aws/config.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	// Create DynamoDB client
	cli := dynamodb.New(sess)
	dbw := &DatabaseWriter{

		CassandraCluster: cassandraCluster,
		CassandraSession: cassandraSession,

		DynamoSession:      sess,
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
	//dbw.submitDeltas(items)
	go dbw.writeDeltasCassandra(items)
}

// writeDelta serializes the OrderBookDelta update and concurrently writes it to the orderbooks table
func (dbw *DatabaseWriter) writeDelta(obd common.OrderBookDelta) {
	items := dbw.extractDeltas(obd)
	//dbw.submitDeltas(items)
	go dbw.writeDeltasCassandra(items)
}

// writeDelta transforms serializes the TradesUpdate and concurrently writes it to the trades table
func (dbw *DatabaseWriter) writeTrades(tu common.TradesUpdate) {
	items := dbw.extractTrades(tu)
	//dbw.submitTrades(items)
	go dbw.writeTradesCassandra(items)
}

func (dbw *DatabaseWriter) writeTradesCassandra(tu []Item) {
	for _, item := range tu {
		dbw.writeWithExponentialBackoffCassandra(item, "Trades")
	}
}

func (dbw *DatabaseWriter) writeDeltasCassandra(tu []Item) {
	for _, item := range tu {
		dbw.writeWithExponentialBackoffCassandra(item, "Orderbooks")
	}
}

func fixExchangeName(old string) string {
	mapping := map[string]string{
		"bitfinex":     "Bitfinex",
		"binance":      "Binance",
		"kraken":       "Kraken",
		"bitstamp":     "Bitstamp",
		"bittrex":      "Bittrex",
		"coinbase-pro": "CoinbasePro",
		"bitmex":       "Bitmex",
	}
	if val, ok := mapping[old]; ok {
		return val
	}
	fmt.Println("Warning: exchange mapping not defined. using default", old)
	return strings.Title(strings.ToLower(old))
}

func fixPair(old string) string {
	s := strings.Split(old, "-")
	pair := s[0]
	var quote string
	if strings.HasSuffix(pair, "usd") {
		quote = "USD"
	} else if strings.HasSuffix(pair, "usdt") {
		quote = "USDT"
	} else if strings.HasSuffix(pair, "btc") {
		quote = "BTC"
	} else if strings.HasSuffix(pair, "eth") {
		quote = "eth"
	} else {
		fmt.Println("Warning: pair mapping not defined. using default", old)
		quote = pair[len(pair)-3:]
	}
	base := pair[:len(pair)-len(quote)]
	newPair := base + "/" + quote
	if len(s) > 1 {
		future := s[1][0:1]
		newPair += "/" + future
	}
	return strings.ToUpper(newPair)
}

func (dbw *DatabaseWriter) writeWithExponentialBackoffCassandra(item Item, tableName string) {
	numOfRetries := 5
	for i := 0; i < numOfRetries; i++ {

		if err := dbw.CassandraSession.Query(`
            INSERT INTO `+tableName+` (exchange, pair, date, ts, price, amount)
            VALUES (?, ?, ?, ?, ?, ?)
            `,
			fixExchangeName(dbw.MarketDescriptor.Exchange),
			fixPair(dbw.MarketDescriptor.Pair),
			time.Unix(0, int64(item.Timestamp)).Format("2006-01-02"),
			time.Unix(0, int64(item.Timestamp)),
			float32(item.Price),
			float32(item.Amount)).Exec(); err != nil {
			fmt.Println("put item throttled with error. retry pending", err)

		} else {
			return
		}

		delay := math.Pow(2.0, float64(i))
		log.Print("retrying in", delay, "seconds")
		time.Sleep(time.Duration(delay) * time.Second)
	}
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
