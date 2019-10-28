package orderbooks

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"github.com/Shopify/sarama"
	"log"
	"math"
	"time"
)

const numWorkers = 1

type DatabaseWriter struct {
	MarketDescriptor   *rest.MarketDescr
	orderbookTableName string
	tradesTableName    string

	snapshotsWriteChan chan []Item
	tradesWriteChan    chan []Item
	deltasWriteChan    chan []Item

	snapshotsWriteQueue []Item
	deltasWriteQueue    []Item
	tradesWriteQueue    []Item

	Producer          sarama.SyncProducer
	snapshotTableName string
}

func NewDatabaseWriter(marketDescriptor *rest.MarketDescr, orderbookTableName string, tradesTableName string, snapshotTableName string, brokers []string) *DatabaseWriter {

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
		snapshotTableName:    snapshotTableName,

		tradesWriteChan:    make(chan []Item, numWorkers),
		deltasWriteChan:    make(chan []Item, numWorkers),
		snapshotsWriteChan: make(chan []Item, numWorkers),

		deltasWriteQueue:    []Item{},
		tradesWriteQueue:    []Item{},
		snapshotsWriteQueue: []Item{},
	}

	for i := 1; i <= numWorkers; i++ {
		go dbw.writer(&dbw.tradesWriteChan, dbw.MarketDescriptor, dbw.tradesTableName)
		go dbw.writer(&dbw.deltasWriteChan, dbw.MarketDescriptor, dbw.orderbookTableName)
		go dbw.writer(&dbw.snapshotsWriteChan, dbw.MarketDescriptor, dbw.tradesTableName)
	}
	return dbw
}

// writer collects the requests from the writeChannel and async writes to Kafka
func (dbw *DatabaseWriter) writer(channel *chan []Item, marketDescriptor *rest.MarketDescr, topic string) {
	for itemBatch := range *channel {
		messages := make([]*sarama.ProducerMessage, len(itemBatch))
		for i, item := range itemBatch {
			messages[i] = item.ConvertForKafka(topic, marketDescriptor.Exchange, marketDescriptor.Pair)
		}
		log.Println("Sending")
		err := dbw.Producer.SendMessages(messages)
		if err != nil {
			log.Println("SendMessages failed", err)
		}
	}
}

// writeDelta serializes the OrderBookDelta update and concurrently writes it to the orderbooks Topic
func (dbw *DatabaseWriter) writeDeltas(item Item) {
	dbw.submitItem(&dbw.deltasWriteQueue, item)
}

// writeDelta transforms serializes the TradesUpdate and concurrently writes it to the trades Topic
func (dbw *DatabaseWriter) writeTrades(item Item) {
	dbw.submitItem(&dbw.tradesWriteQueue, item)
}

func (dbw *DatabaseWriter) writeSnapshots(item Item) {
	dbw.submitItem(&dbw.tradesWriteQueue, item)
}

// submitItems appends the requests to the queue which is then potentially sent to the writer channel
func (dbw *DatabaseWriter) submitItem(queue *[]Item, item Item) {
	dbw.deltasWriteQueue = append(dbw.deltasWriteQueue, item)
	queueLength := len(dbw.deltasWriteQueue)
	if queueLength > 0 {
		// dont block if chan is full. the queued requests will be processed later
		select {
		case dbw.deltasWriteChan <- dbw.deltasWriteQueue:
			{
				dbw.deltasWriteQueue = nil
			}
		default:
		}
	}
}
