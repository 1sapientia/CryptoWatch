package orderbooks

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"math"
	"time"
)

const numWorkers = 1

type Item struct {
	Table     string
	Timestamp int64
	Price     string
	Amount    string //(positive bid, negative ask)
}

type DatabaseWriter struct {
	CassandraSession *gocql.Session
	CassandraCluster *gocql.ClusterConfig

	MarketDescriptor   *rest.MarketDescr
	orderbookTableName string
	tradesTableName    string

	writeChan          chan []Item
	writeQueue         []Item
	ExchangeDescriptor *rest.ExchangeDescr
	PairDescriptor     *rest.PairDescr
}

func NewDatabaseWriter(marketDescriptor *rest.MarketDescr, exchangeDescriptor *rest.ExchangeDescr, pairDescriptor *rest.PairDescr, orderbookTableName string, tradesTableName string) *DatabaseWriter {

	cassandraCluster := gocql.NewCluster("127.0.0.1")
	cassandraCluster.Keyspace = "orderbookretriever"
	cassandraSession, err := cassandraCluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	dbw := &DatabaseWriter{

		CassandraCluster: cassandraCluster,
		CassandraSession: cassandraSession,

		MarketDescriptor:   marketDescriptor,
		ExchangeDescriptor:   exchangeDescriptor,
		PairDescriptor:   pairDescriptor,
		orderbookTableName: orderbookTableName,
		tradesTableName:    tradesTableName,

		writeChan:  make(chan []Item, numWorkers),
		writeQueue: []Item{},
	}

	for i := 1; i <= numWorkers; i++ {
		go dbw.writer()
	}

	return dbw
}

// writer collects the requests from the writeChannel and writes in batches
func (dbw *DatabaseWriter) writer() {
	for itemBatch := range dbw.writeChan {
		for _, item := range itemBatch {
			dbw.writeWithExponentialBackoffCassandra(item)
		}
	}
}

// writeCheckpoint writes the checkpoint item to the database
func (dbw *DatabaseWriter) writeCheckpoint() {
	items := []Item{{
		Table:     dbw.orderbookTableName,
		Timestamp: time.Now().UnixNano(),
		Price:     "C",
		Amount:    "0",
	}}
	fmt.Println("writing checkpoint", dbw.MarketDescriptor, time.Now())
	dbw.submitItems(items)
}

// writeDelta serializes the OrderBookDelta update and concurrently writes it to the orderbooks table
func (dbw *DatabaseWriter) writeDelta(obd common.OrderBookDelta) {
	items := dbw.extractDeltas(obd)
	dbw.submitItems(items)
}

// writeDelta transforms serializes the TradesUpdate and concurrently writes it to the trades table
func (dbw *DatabaseWriter) writeTrades(tu common.TradesUpdate) {
	items := dbw.extractTrades(tu)
	dbw.submitItems(items)
}

// submitItems appends the requests to the queue which is then potentially sent to the writer channel
func (dbw *DatabaseWriter) submitItems(items []Item) {
	dbw.writeQueue = append(dbw.writeQueue, items...)
	queueLength := len(dbw.writeQueue)
	if queueLength > 100 {
		// dont block if chan is full. the queued requests will be processed later
		select {
		case dbw.writeChan <- dbw.writeQueue[:100]:
			{
				dbw.writeQueue = dbw.writeQueue[100:]
			}
		default:
		}
	}
}

// writeWithExponentialBackoffCassandra dispatches the request batch with retries
func (dbw *DatabaseWriter) writeWithExponentialBackoffCassandra(item Item) {
	numOfRetries := 5
	for i := 0; i < numOfRetries; i++ {
		//fmt.Println(item, dbw.PairDescriptor, dbw.ExchangeDescriptor)
		if err := dbw.CassandraSession.Query(`
            INSERT INTO `+item.Table+` (exchange, pair, date, ts, price, amount)
            VALUES (?, ?, ?, ?, ?, ?)
            `,
			dbw.ExchangeDescriptor.ID,
			dbw.PairDescriptor.ID,
			time.Unix(0, item.Timestamp).Format("2006-01-02"),
			item.Timestamp,
			item.Price,
			item.Amount).Exec(); err != nil {
			fmt.Println("put item throttled with error. retry pending", err)

		} else {
			print("written")
			return
		}

		delay := math.Pow(2.0, float64(i))
		log.Print("retrying in", delay, "seconds")
		time.Sleep(time.Duration(delay) * time.Second)
	}
}

// extractTrades serializes the TradesUpdate to a list of Items
func (dbw *DatabaseWriter) extractTrades(tu common.TradesUpdate) []Item {
	var trades []Item
	parseTrade := func(newTrade common.PublicTrade) {
		trades = append(trades, Item{
			Table:     dbw.tradesTableName,
			Timestamp: tu.Timestamp.UnixNano(),
			Amount:    newTrade.Amount,
			Price:     newTrade.Price,
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
	ts := obd.Timestamp.UnixNano()
	parseRemovals := func(removePrice string, isAsk bool) {
		price := removePrice
		amount := "0" // remove
		if isAsk {
			amount = "-"+amount
			price = "-"+price
		}
		deltas = append(deltas, Item{
			Table:     dbw.orderbookTableName,
			Timestamp: ts,
			Price:     price,
			Amount:    amount,
		})
	}

	parseOrders := func(newOrder common.PublicOrder, isAsk bool) {
		price := newOrder.Price
		amount := newOrder.Amount
		if isAsk {
			amount = "-"+amount
			price = "-"+price
		}
		deltas = append(deltas, Item{
			Table:     dbw.orderbookTableName,
			Timestamp: ts,
			Price:     price,
			Amount:    amount,
		})
	}

	for _, removePrice := range obd.Asks.Remove {
		parseRemovals(removePrice, true)
	}
	for _, removePrice := range obd.Bids.Remove {
		parseRemovals(removePrice, false)
	}

	for _, newOrder := range obd.Asks.Set {
		parseOrders(newOrder, true)
	}
	for _, newOrder := range obd.Bids.Set {
		parseOrders(newOrder, false)
	}

	return deltas
}
