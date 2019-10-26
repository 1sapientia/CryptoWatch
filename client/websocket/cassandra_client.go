package websocket

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultCassandraURL      = "wss://stream.cryptowat.ch"
	DefaultCassandraKeyspace = "orderbookretriever"
)

// CassandraClient is used to connect to Cryptowatch's data streaming backend.
// Typically you will get an instance using NewCassandraClient(), set any state
// listeners for the connection you might need, then set data listeners for
// whatever data subscriptions you have. Finally, you can call Connect() to
// initiate the data stream.
type CassandraClient struct {
	cassandraSession *gocql.Session
	cassandraCluster *gocql.ClusterConfig

	marketUpdateListeners []MarketUpdateCB

	callMarketUpdateListeners chan callMarketUpdateListenersReq

	mtx    sync.Mutex
	waitgroup sync.WaitGroup

	params *CassandraClientParams
}

// WSParams contains options for opening a websocket connection.
type CassandraParams struct {
	URL      string
	Keyspace string
}

type CassandraClientParams struct {
	CassandraParams    *CassandraParams
	Markets            map[int]rest.MarketDescr
	Subscriptions      []*StreamSubscription
	StartTime          time.Time
	EndTime            time.Time
	OrderbookTableName string
	TradesTableName    string
}

// NewCassandraClient creates a new CassandraClient instance with the given params.
// Although it starts listening for data immediately, you will still have to
// register listeners to handle that data, and then call Connect() explicitly.
func NewCassandraClient(params *CassandraClientParams) (*CassandraClient, error) {
	// Make a copy of params struct because we might alter it below
	paramsCopy := *params
	params = &paramsCopy

	if params.CassandraParams.URL == "" {
		params.CassandraParams.URL = DefaultCassandraURL
	}

	if params.CassandraParams.URL == "" {
		params.CassandraParams.Keyspace = DefaultCassandraKeyspace
	}

	cassandraCluster := gocql.NewCluster(params.CassandraParams.URL)
	cassandraCluster.Keyspace = params.CassandraParams.Keyspace
	cassandraSession, err := cassandraCluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	sc := &CassandraClient{
		params:                    params,
		cassandraSession:          cassandraSession,
		callMarketUpdateListeners: make(chan callMarketUpdateListenersReq, 1),
	}

	go sc.listen()

	return sc, nil
}

// listen is used internally to dispatch data to registered listeners.
func (sc *CassandraClient) listen() {
	for {
		select {
		case req := <-sc.callMarketUpdateListeners:
			for _, l := range req.listeners {
				l(req.market, req.update)
			}
		}
	}
}

// Market listeners

// OnMarketUpdate sets a callback for all market updates. MarketUpdateCB
// contains MarketUpdate, which is a container for every type of update. For each
// MarketUpdate, it will contain exactly one non-nil struct, which is one of the
// following:
// OrderBookSnapshot
// OrderBookDelta
// OrderBookSpreadUpdate
// TradesUpdate
// IntervalsUpdate
// SummaryUpdate
// SparklineUpdate
func (sc *CassandraClient) OnMarketUpdate(cb MarketUpdateCB) {
	sc.mtx.Lock()
	defer sc.mtx.Unlock()

	sc.marketUpdateListeners = append(sc.marketUpdateListeners, cb)
}

// OnStateChange registers a new listener for the given state. The listener is
// registered with the default options (call the listener every time the state
// becomes active, and don't call the listener immediately for the current
// state). All registered callbacks for all states (and all messages, see
// OnMarketUpdate) will be called by the same internal goroutine, i.e. they are
// never called concurrently with each other.
//
// The order of listeners invocation for the same state is unspecified, and
// clients shouldn't rely on it.
//
// The listeners shouldn't block; a blocked listener will also block the whole
// stream connection.
//
// To subscribe to all state changes, use ConnStateAny as a state.
func (sc *CassandraClient) OnStateChange(state ConnState, cb StateCallback) {
}

// Connect either starts a connection goroutine (if state is
// ConnStateDisconnected), or makes it connect immediately, ignoring timeout
// (if the state is ConnStateWaitBeforeReconnect). For other states, this returns an
// error.
//
// Connect doesn't wait for the connection to establish; it returns immediately.
func (sc *CassandraClient) Connect() (err error) {

	sc.mtx.Lock()
	marketListeners := make([]MarketUpdateCB, len(sc.marketUpdateListeners))
	copy(marketListeners, sc.marketUpdateListeners)
	sc.mtx.Unlock()

	for _, sub := range sc.params.Subscriptions {
		stream, marketId, exchange, pair := sc.parseSubscription(sub)
		switch stream {
			case "deltas":
				sc.waitgroup.Add(1)
				go sc.queryCassandraDeltas(marketId, exchange, pair, marketListeners)
			case "trades":
				sc.waitgroup.Add(1)
				go sc.queryCassandraTrades(marketId, exchange, pair, marketListeners)
		}
	}

	go func() {
		sc.waitgroup.Wait()
		_ = sc.Close()
		log.Print("Cassandra Querying Done")
		os.Exit(0)
	}()

	return nil
}

// Close stops the connection (or reconnection loop, if active), and if
// websocket connection is active at the moment, closes it as well.
func (sc *CassandraClient) Close() (err error) {
	sc.cassandraSession.Close()
	return nil
}

func (sc *CassandraClient) queryCassandraDeltas(marketId string, exchange string, pair string, listeners []MarketUpdateCB) {
	defer sc.waitgroup.Done()
	var ts time.Time
	var price float32
	var amount float32

	// subtract 24 hours from the StartTime to make sure that the midnight full snapshot is captured
	for date := sc.params.StartTime.Add(time.Hour * -24); 0 < sc.params.EndTime.Sub(date).Hours()/24; date = date.Add(time.Hour * 24) {
		iter := sc.cassandraSession.Query(
			fmt.Sprintf(`SELECT ts, price, amount 
                                FROM %s 
                                WHERE exchange=? and pair=? and date = ? and ts > ? and ts < ? order by ts`, sc.params.OrderbookTableName),
			common.FixExchangeName(exchange),
			common.FixPair(pair),
			date.Format("2006-01-02"),
			sc.params.StartTime,
			sc.params.EndTime).Iter()

		for iter.Scan(&ts, &price, &amount) {

			sc.callMarketUpdateListeners <- callMarketUpdateListenersReq{
				market: common.Market{ID: common.MarketID(marketId)},
				update: common.MarketUpdate{
					CassandraDelta: &common.CassandraDelta{
						Timestamp: ts,
						Price:     price,
						Amount:    amount,
				}},
				listeners: listeners,
			}
		}
	}
}

func (sc *CassandraClient) queryCassandraTrades(marketId string, exchange string, pair string, listeners []MarketUpdateCB) {
	defer sc.waitgroup.Done()
	var ts time.Time
	var price float32
	var amount float32

	for date := sc.params.StartTime; 0 < sc.params.EndTime.Sub(date).Hours()/24; date = date.Add(time.Hour * 24) {
		fmt.Println(date)

		iter := sc.cassandraSession.Query(
			fmt.Sprintf(`SELECT ts, price, amount 
                                FROM %s 
                                WHERE exchange=? and pair=? and date = ? and ts > ? and ts < ? order by ts`, sc.params.TradesTableName),
			common.FixExchangeName(exchange),
			common.FixPair(pair),
			date.Format("2006-01-02"),
			sc.params.StartTime,
			sc.params.EndTime).Iter()

		for iter.Scan(&ts, &price, &amount) {
			sc.callMarketUpdateListeners <- callMarketUpdateListenersReq{
				market: common.Market{ID: common.MarketID(marketId)},
				update: common.MarketUpdate{
					CassandraTrade: &common.CassandraTrade{
						Timestamp: ts,
						Price:     price,
						Amount:    amount,
					}},
				listeners: listeners,
			}
		}
	}
}


func (sc *CassandraClient) parseSubscription(subscription *StreamSubscription) (string, string, string, string) {
	//subscription.Resource:  "markets:%d:book:snapshots"
	subscriptionParts := strings.Split(subscription.Resource, ":")
	stream := subscriptionParts[len(subscriptionParts)-1]
	marketId, err := strconv.Atoi(subscriptionParts[1])
	if err != nil {
		panic("marketId should be int")
	}

	exchange := sc.params.Markets[marketId].Exchange
	pair := sc.params.Markets[marketId].Pair

	return stream, subscriptionParts[1], exchange, pair

}

