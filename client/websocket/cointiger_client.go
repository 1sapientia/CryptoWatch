package websocket

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"math"
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

// CointigerClient is used to connect to Cryptowatch's data streaming backend.
// Typically you will get an instance using NewCassandraClient(), set any state
// listeners for the connection you might need, then set data listeners for
// whatever data subscriptions you have. Finally, you can call Connect() to
// initiate the data stream.
type CointigerClient struct {

	marketUpdateListeners []MarketUpdateCB

	callMarketUpdateListeners chan callMarketUpdateListenersReq

	mtx    sync.Mutex
	waitgroup sync.WaitGroup

	params *CointigerClientParams
}



type CointigerClientParams struct {
	Markets            map[int]rest.MarketDescr
	Subscriptions      []*StreamSubscription
	StartTime          time.Time
	EndTime            time.Time
	OrderbookTableName string
	TradesTableName    string
}

// NewCassandraClient creates a new CointigerClient instance with the given params.
// Although it starts listening for data immediately, you will still have to
// register listeners to handle that data, and then call Connect() explicitly.
func NewCassandraClient(params *CointigerClientParams) (*CointigerClient, error) {
	// Make a copy of params struct because we might alter it below
	paramsCopy := *params
	params = &paramsCopy



	sc := &CointigerClient{
		params:                    params,
		callMarketUpdateListeners: make(chan callMarketUpdateListenersReq, 1),
	}

	go sc.listen()

	return sc, nil
}

// listen is used internally to dispatch data to registered listeners.
func (sc *CointigerClient) listen() {
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
func (sc *CointigerClient) OnMarketUpdate(cb MarketUpdateCB) {
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
func (sc *CointigerClient) OnStateChange(state ConnState, cb StateCallback) {
}

// Connect either starts a connection goroutine (if state is
// ConnStateDisconnected), or makes it connect immediately, ignoring timeout
// (if the state is ConnStateWaitBeforeReconnect). For other states, this returns an
// error.
//
// Connect doesn't wait for the connection to establish; it returns immediately.
func (sc *CointigerClient) Connect() (err error) {

	sc.mtx.Lock()
	marketListeners := make([]MarketUpdateCB, len(sc.marketUpdateListeners))
	copy(marketListeners, sc.marketUpdateListeners)
	sc.mtx.Unlock()

	for _, sub := range sc.params.Subscriptions {
		stream, marketId, exchange, pair := sc.parseSubscription(sub)
		switch stream {
			case "deltas":
				submitDeltasUpdateListeners := make(chan callMarketUpdateListenersReq, 1000)
				submitTradesUpdateListeners := make(chan callMarketUpdateListenersReq, 1000)
				go sc.queryCassandraDeltas(marketId, exchange, pair, marketListeners, submitDeltasUpdateListeners)
				go sc.queryCassandraTrades(marketId, exchange, pair, marketListeners, submitTradesUpdateListeners)
				sc.waitgroup.Add(1)
				go sc.merge(submitTradesUpdateListeners, submitDeltasUpdateListeners)
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
func (sc *CointigerClient) Close() (err error) {
	return nil
}



func orderBookDeltaUpdateFromCassandra(delta *common.OrderBookDelta, ts time.Time, price float32, amount float32) {

	startTime, _ := time.Parse("2006-01-02 15:04:05.000", "2019-10-31 20:03:41.977")
	EndTime, _ := time.Parse("2006-01-02 15:04:05.000", "2019-10-31 20:04:00.977")

	if ts.Before(EndTime)&&ts.After(startTime){
		//fmt.Println(ts, price, amount)
	}

	if price==183.93{
		//fmt.Println(ts, amount)
	}
	p := fmt.Sprintf("%.5g",  math.Abs(float64(price)))
	a := fmt.Sprintf("%.5g", math.Abs(float64(amount)))
	if amount > 0{
		delta.Bids.Set = append(delta.Bids.Set, common.PublicOrder{
			Price:  p,
			Amount: a,
		})
	}else if amount < 0{
		delta.Asks.Set = append(delta.Asks.Set, common.PublicOrder{
			Price:  p,
			Amount: a,
		})
	}else{
		if price < 0{
			delta.Asks.Remove = append(delta.Asks.Remove, p)

		} else {
			delta.Bids.Remove = append(delta.Bids.Remove, p)


		}
	}
	delta.Timestamp=ts
}

func tradesUpdateFromCassandra(ts time.Time, price float32, amount float32) common.TradesUpdate {
	p := fmt.Sprintf("%.5g", price)
	a := fmt.Sprintf("%.5g", amount)
	return common.TradesUpdate{
		Timestamp:ts,
		Trades:[]common.PublicTrade{{
			Price:  p,
			Amount: a,
		}},
	}
}

func (sc *CointigerClient) parseSubscription(subscription *StreamSubscription) (string, string, string, string) {
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

