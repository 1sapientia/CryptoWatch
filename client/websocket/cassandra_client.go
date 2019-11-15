package websocket

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"os"
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

// listen is used internally to dispatch data to registered listeners.
func (sc *CassandraClient) merge(submitTradesUpdateListeners <-chan callMarketUpdateListenersReq, submitDeltasUpdateListeners <-chan callMarketUpdateListenersReq) {
	defer sc.waitgroup.Done()
	var tradeUpdate *callMarketUpdateListenersReq
	var deltaUpdate *callMarketUpdateListenersReq
	for {
		if tradeUpdate == nil{
			//fmt.Println("waiting trades")
			t, open := <-submitTradesUpdateListeners
			if open{
				tradeUpdate = &t
			}
		}
		if deltaUpdate == nil{
			//fmt.Println("waiting deltas")
			d, open := <-submitDeltasUpdateListeners
			if open{
				deltaUpdate = &d
			}
		}
		//fmt.Println("processing")
		if tradeUpdate == nil && deltaUpdate == nil{
			return
		}

		if tradeUpdate == nil || (deltaUpdate!=nil && deltaUpdate.update.OrderBookDelta.Timestamp.Before(tradeUpdate.update.TradesUpdate.Timestamp)){
			sc.callMarketUpdateListeners <- *deltaUpdate
			deltaUpdate = nil
		} else {
			sc.callMarketUpdateListeners <- *tradeUpdate
			tradeUpdate = nil
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
		//log.Print("Cassandra Querying Done")
		fmt.Print("]")
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

func (sc *CassandraClient) queryCassandraDeltas(marketId string, exchange rest.ExchangeDescr, pair rest.PairDescr, listeners []MarketUpdateCB, submitDeltasUpdateListeners chan<- callMarketUpdateListenersReq) {
	defer close(submitDeltasUpdateListeners)
	var ts int64
	var price string
	var amount string

	startTime := sc.params.StartTime.Add(time.Minute * -24)
	date := startTime

	update := common.OrderBookDelta{
		Timestamp: time.Time{},
		Bids:      common.OrderDeltas{
			Set:    []common.PublicOrder{},
			Remove: []string{},
		},
		Asks:      common.OrderDeltas{
			Set:    []common.PublicOrder{},
			Remove: []string{},
		},
	}

	// subtract 24 mins from the StartTime to make sure that the midnight full snapshot is captured
	for {
		if 0 > sc.params.EndTime.Sub(date).Hours()/24{
			break
		}
		fmt.Println(date, exchange.Name, pair.Symbol)
		iter := sc.cassandraSession.Query(
			fmt.Sprintf(`SELECT ts, price, amount 
                                FROM %s 
                                WHERE exchange=? and pair=? and date = ? and ts >= ? and ts < ? order by ts`, sc.params.OrderbookTableName),
			exchange.ID,
			pair.ID,
			date.Format("2006-01-02"),
			startTime.UnixNano(),
			sc.params.EndTime.UnixNano()).Iter()

		for iter.Scan(&ts, &price, &amount) {
			if price == "C"{
				fmt.Println(ts, "checkpoint")
				continue
			}
			if ts-startTime.UnixNano()>=1{

				//fmt.Println(time.Unix(0, ts))
				if !update.Timestamp.IsZero(){
					u := update
					submitDeltasUpdateListeners <- callMarketUpdateListenersReq{
						market: common.Market{ID: common.MarketID(marketId)},
						update: common.MarketUpdate{OrderBookDelta:&u},
						listeners: listeners,
					}
				}
				update = common.OrderBookDelta{
					Timestamp: time.Time{},
					Bids:      common.OrderDeltas{
						Set:    []common.PublicOrder{},
						Remove: []string{},
					},
					Asks:      common.OrderDeltas{
						Set:    []common.PublicOrder{},
						Remove: []string{},
					},
				}
			}
			orderBookDeltaUpdateFromCassandra(&update, ts, price, amount)
			startTime = time.Unix(0, ts)
		}
		if err := iter.Close(); err != nil{
			fmt.Println(err, "retry from", startTime)
			time.Sleep(time.Second*10)
			continue
		}
		date = date.Add(time.Hour * 24)
	}
}

func orderBookDeltaUpdateFromCassandra(delta *common.OrderBookDelta, ts int64, price string, amount string) {
	if strings.HasPrefix(price, "-"){
		price = strings.TrimPrefix(price, "-")
		amount = strings.TrimPrefix(amount, "-")
		if amount == "0"{
			delta.Asks.Remove = append(delta.Asks.Remove, price)
		} else {
			delta.Asks.Set = append(delta.Asks.Set, common.PublicOrder{
				Price:  price,
				Amount: amount,
			})
		}
	}else{
		if amount == "0"{
			delta.Bids.Remove = append(delta.Bids.Remove, price)
		} else {
			delta.Bids.Set = append(delta.Bids.Set, common.PublicOrder{
				Price:  price,
				Amount: amount,
			})
		}
	}
	delta.Timestamp=time.Unix(0, ts)
}

func (sc *CassandraClient) queryCassandraTrades(marketId string, exchange rest.ExchangeDescr, pair rest.PairDescr, listeners []MarketUpdateCB, submitTradesUpdateListeners chan<- callMarketUpdateListenersReq) {
	defer 	close(submitTradesUpdateListeners)
	var ts int64
	var price string
	var amount string

	startTime := sc.params.StartTime
	date := startTime
	for {
		if 0 > sc.params.EndTime.Sub(date).Hours()/24{
			break
		}
		iter := sc.cassandraSession.Query(
			fmt.Sprintf(`SELECT ts, price, amount 
                                FROM %s 
                                WHERE exchange=? and pair=? and date = ? and ts > ? and ts < ? order by ts`, sc.params.TradesTableName),
			exchange.ID,
			pair.ID,
			date.Format("2006-01-02"),
			startTime.UnixNano(),
			sc.params.EndTime.UnixNano()).Iter()

		for iter.Scan(&ts, &price, &amount) {
			update := tradesUpdateFromCassandra(ts, price, amount)
			submitTradesUpdateListeners <- callMarketUpdateListenersReq{
				market: common.Market{ID: common.MarketID(marketId)},
				update: common.MarketUpdate{TradesUpdate:&update},
				listeners: listeners,
			}
			startTime = time.Unix(0, ts)
		}

		if err := iter.Close(); err != nil{
			fmt.Println(err, "retry from", startTime)
			time.Sleep(time.Second*10)
			continue
		}
		date = date.Add(time.Hour * 24)
	}

}

func tradesUpdateFromCassandra(ts int64, price string, amount string) common.TradesUpdate {
	return common.TradesUpdate{
		Timestamp:time.Unix(0, ts),
		Trades:[]common.PublicTrade{{
			Price:  price,
			Amount: amount,
		}},
	}
}


func (sc *CassandraClient) parseSubscription(subscription *StreamSubscription) (string, string, rest.ExchangeDescr, rest.PairDescr) {
	//subscription.Resource:  "markets:%d:book:snapshots"
	subscriptionParts := strings.Split(subscription.Resource, ":")
	stream := subscriptionParts[len(subscriptionParts)-1]

	exchange := subscription.ExchangeDescriptor
	pair := subscription.PairDescriptor

	return stream, subscriptionParts[1], exchange, pair

}

