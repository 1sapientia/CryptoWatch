package orderbooks

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"math"
	"strconv"
	"strings"
	"time"
)

const numWorkers = 10

type Item struct {
	Table     string
	Timestamp float64 //market id
	Price     float64 //timestamp (ms)
	Amount    float64 //amount (positive bid, negative ask)
}

type DatabaseWriter struct {
	CassandraSession *gocql.Session
	CassandraCluster *gocql.ClusterConfig

	MarketDescriptor   *rest.MarketDescr
	orderbookTableName string
	tradesTableName    string

	writeChan  chan []Item
	writeQueue []Item
}

func NewDatabaseWriter(marketDescriptor *rest.MarketDescr, orderbookTableName string, tradesTableName string) *DatabaseWriter {

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
		Timestamp: float64(time.Now().UnixNano()),
		Price:     0, // price zero indicates the checkpoint
		Amount:    0,
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
	if queueLength > 1000 {
		// dont block if chan is full. the queued requests will be processed later
		select {
		case dbw.writeChan <- dbw.writeQueue[:1000]:
			{
				dbw.writeQueue = dbw.writeQueue[1000:]
			}
		default:
		}
	}
}

//fixExchangeName maps the cryptowatch exchange names to correct format (camelcase)
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

//fixPair maps the cryptowatch pair names to correct format (BASE/QUOTE/potential[Q,P,W] for futures)
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

// writeWithExponentialBackoffCassandra dispatches the request batch with retries
func (dbw *DatabaseWriter) writeWithExponentialBackoffCassandra(item Item) {
	numOfRetries := 5
	for i := 0; i < numOfRetries; i++ {

		if err := dbw.CassandraSession.Query(`
            INSERT INTO `+item.Table+` (exchange, pair, date, ts, price, amount)
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
			Table:     dbw.tradesTableName,
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
			Table:     dbw.orderbookTableName,
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
			Table:     dbw.orderbookTableName,
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
