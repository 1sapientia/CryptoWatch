package orderbooks

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
	"code.cryptowat.ch/cw-sdk-go/proto/kafka"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"log"
	"math"
	"strconv"
	"strings"
	"time"
)

const numWorkers = 1

type Item struct {
	Table     string
	Timestamp float64 //market id
	Price     float64 //timestamp (ms)
	Amount    float64 //amount (positive bid, negative ask)
}

type DatabaseWriter struct {
	MarketDescriptor   *rest.MarketDescr
	orderbookTableName string
	tradesTableName    string

	writeChan  chan []Item
	writeQueue []Item
	Producer   sarama.SyncProducer
}

func NewDatabaseWriter(marketDescriptor *rest.MarketDescr, orderbookTableName string, tradesTableName string, brokers []string) *DatabaseWriter {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 5
	config.Producer.Retry.BackoffFunc = func(retries, maxRetries int) time.Duration {
		delay := math.Pow(2.0, float64(retries))
		return time.Duration(delay) * time.Second
	}
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		// Should not reach here
		panic(err)
	}

	dbw := &DatabaseWriter{

		Producer: producer,

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

// writer collects the requests from the writeChannel and async writes to Kafka
func (dbw *DatabaseWriter) writer() {
	for itemBatch := range dbw.writeChan {
		messages := make([]*sarama.ProducerMessage, len(itemBatch))
		for i, item := range itemBatch {
			messages[i] = dbw.GenerateProducerMessage(item)
		}
		log.Println("Sending")
		err := dbw.Producer.SendMessages(messages)
		if err != nil {
			log.Println("SendMessages failed", err)
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

// writeDelta serializes the OrderBookDelta update and concurrently writes it to the orderbooks Topic
func (dbw *DatabaseWriter) writeDelta(obd common.OrderBookDelta) {
	items := dbw.extractDeltas(obd)
	dbw.submitItems(items)
}

// writeDelta transforms serializes the TradesUpdate and concurrently writes it to the trades Topic
func (dbw *DatabaseWriter) writeTrades(tu common.TradesUpdate) {
	items := dbw.extractTrades(tu)
	dbw.submitItems(items)
}

// submitItems appends the requests to the queue which is then potentially sent to the writer channel
func (dbw *DatabaseWriter) submitItems(items []Item) {
	dbw.writeQueue = append(dbw.writeQueue, items...)
	queueLength := len(dbw.writeQueue)
	if queueLength > 0 {
		// dont block if chan is full. the queued requests will be processed later
		select {
		case dbw.writeChan <- dbw.writeQueue:
			{
				dbw.writeQueue = nil
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

//GenerateProducerMessage generates Kafka message to send via producer
func (dbw *DatabaseWriter) GenerateProducerMessage(item Item) *sarama.ProducerMessage {

	timestampProto, _ := ptypes.TimestampProto(time.Unix(0, int64(item.Timestamp)))
	messageKey := &kafka.MessageKey{
		Exchange: fixExchangeName(dbw.MarketDescriptor.Exchange),
		Pair:     fixPair(dbw.MarketDescriptor.Pair),
	}

	messageValue := &kafka.MessageValue{
		Price:     float32(item.Price),
		Amount:    float32(item.Amount),
		Timestamp: timestampProto,
	}

	key, err1 := proto.Marshal(messageKey)
	value, err2 := proto.Marshal(messageValue)
	if err1 != nil || err2 != nil {
		log.Println("Proto marshaling failed", err1, err2)
	}

	return &sarama.ProducerMessage{
		Topic: item.Table,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
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
