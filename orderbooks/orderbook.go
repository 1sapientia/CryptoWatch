package orderbooks

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
	"fmt"
	"github.com/juju/errors"
	"log"
	"sort"
	"strconv"
	"time"
)

var (
	// ErrSeqNumMismatch is returned
	ErrSeqNumMismatch = errors.New("seq num mismatch")
)

// OrderBook represents a "live" order book, which is able to receive snapshots
// and deltas.
//
// It is not thread-safe; so if you need to use it from more than one
// goroutine, apply your own synchronization.
type OrderBook struct {
	intervalTrades   []Trade
	intervalDeltas   []Delta
	snapshot         common.OrderBookSnapshot
	marketDescriptor rest.MarketDescr
	lastCheckpoint   time.Time
}

func NewOrderBook(snapshot common.OrderBookSnapshot) *OrderBook {
	return &OrderBook{
		snapshot: snapshot,
	}
}

// GetSnapshot returns the snapshot of the current orderbook.
func (ob *OrderBook) GetSnapshot() common.OrderBookSnapshot {
	return ob.snapshot
}

// GetSeqNum is a shortcut for GetSnapshot().SeqNum
func (ob *OrderBook) GetSeqNum() common.SeqNum {
	return ob.snapshot.SeqNum
}

// ApplyDelta applies the given delta (received from the wire) to the current
// orderbook. If the sequence number isn't exactly the old one incremented by
// 1, returns an error without applying delta.
func (ob *OrderBook) ApplyDelta(obd common.OrderBookDelta, writer *DatabaseWriter) error {
	return ob.ApplyDeltaOpt(obd, false, writer)
}

// ApplyDeltaOpt applies the given delta (received from the wire) to the
// current orderbook. If ignoreSeqNum is true, applies the delta even if the
// sequence number isn't exactly the old one incremented by 1.
func (ob *OrderBook) ApplyDeltaOpt(obd common.OrderBookDelta, ignoreSeqNum bool, writer *DatabaseWriter) error {
	// Refuse to apply delta of there is a gap in sequence numbers
	if !ignoreSeqNum && obd.SeqNum-1 != ob.snapshot.SeqNum {
		return ErrSeqNumMismatch
	}


	deltaItems := ob.extractDeltas(obd)

	ob.intervalDeltas = append(ob.intervalDeltas, deltaItems...)

	if writer != nil {
		for _, delta := range (deltaItems){
			writer.writeDeltas(delta)
		}
	}
	startTime, _ := time.Parse("2006-01-02 15:04:05.000", "2019-11-03 04:31:57.977")
	EndTime, _ := time.Parse("2006-01-02 15:04:05.000", "2019-11-03 04:33:00.977")

	if obd.Timestamp.Before(EndTime)&&obd.Timestamp.After(startTime){
		//fmt.Println(obd, len(deltaItems))
	}

	ob.snapshot.Bids = ordersWithDelta(ob.snapshot.Bids, &obd.Bids, true)
	ob.snapshot.Asks = ordersWithDelta(ob.snapshot.Asks, &obd.Asks, false)


	if obd.Timestamp.Before(EndTime)&&obd.Timestamp.After(startTime){
		//fmt.Println(obd, len(deltaItems))
	}

	if len( ob.snapshot.Asks)>0 && len(ob.snapshot.Bids)>0 && ob.snapshot.Asks[0].Price<ob.snapshot.Bids[0].Price{
		fmt.Println(ob.snapshot.Asks[0].Price, ob.snapshot.Bids[0].Price , obd.Timestamp, len(deltaItems))
	}


	//ob.snapshot.SeqNum = obd.SeqNum
	ob.snapshot.SeqNum += 1

	return nil
}

// ApplySnapshot sets the internal orderbook to the provided snapshot.
func (ob *OrderBook) ApplySnapshot(snapshot common.OrderBookSnapshot, writer *DatabaseWriter) {

	snapshotDeltas := snapshot.GetDeltasAgainst(ob.snapshot)
	_ = ob.ApplyDeltaOpt(snapshotDeltas, true, writer)

	//old code:
	//ob.snapshot = snapshot
}

// SetSnapshotCheckpoint writes the checkpoint item to the database if the writer is not nil
func (ob *OrderBook) SetSnapshotCheckpoint(writer *DatabaseWriter) {
	if writer != nil {
		writer.writeCheckpoint()
	}
}

// IsTimeForCheckpoint checks if we need a new snapshot checkpoint
func (ob *OrderBook) IsTimeForCheckpoint() bool {
	return ob.lastCheckpoint.Day() != time.Now().Day()
}

// ordersWithDelta applies given deltas to the slice of orders, and returns a
// newly allocated slice of orders, sorted by price accordingly to reverse
// argument.
func ordersWithDelta(
	orders []common.PublicOrder, deltas *common.OrderDeltas, reverse bool,
) []common.PublicOrder {
	setMap := map[string]common.PublicOrder{}
	removeMap := map[string]struct{}{}

	for _, newOrder := range deltas.Set {
		setMap[newOrder.Price] = newOrder
	}

	for _, removePrice := range deltas.Remove {
		removeMap[removePrice] = struct{}{}
	}

	// At this point we can't know for sure how many orders we'll have in the end
	// (because some of the "Set" deltas can replace existing orders), but we'll
	// allocate it so that it's certainly enough.
	ocap := len(orders) + len(setMap) - len(removeMap)
	if ocap < 0 {
		ocap = 0
	}

	newOrders := make([]common.PublicOrder, 0, ocap)

	// Replace / remove existing orders
	for _, order := range orders {
		if _, ok := removeMap[order.Price]; ok {
			// Need to remove this order, so don't add it
			continue
		}

		if newOrder, ok := setMap[order.Price]; ok {
			// Need to adjust the amount in that order
			order.Amount = newOrder.Amount

			// Also delete the order from setMap, so that we know we already took
			// care of that order.
			delete(setMap, order.Price)
		}

		newOrders = append(newOrders, order)
	}

	// Add new orders (which are still in setMap)
	for _, order := range setMap {
		if _, ok := removeMap[order.Price]; ok {
			// Need to remove this order, so don't add it
			continue
		}
		newOrders = append(newOrders, order)
	}

	// Sort results
	if !reverse {
		sort.Sort(common.PublicOrdersByPrice(newOrders))
	} else {
		sort.Sort(sort.Reverse(common.PublicOrdersByPrice(newOrders)))
	}

	return newOrders
}

// extractTrades serializes the TradesUpdate to a list of Items
func (ob *OrderBook) extractTrades(tu common.TradesUpdate) []Trade {
	var trades []Trade

	parseTrade := func(newTrade common.PublicTrade) {
		amount, err1 := strconv.ParseFloat(newTrade.Amount, 64)
		price, err2 := strconv.ParseFloat(newTrade.Price, 64)
		if err1 != nil || err2 != nil {
			log.Print("trade string to float conversion failed", err1, err2)
			return
		}
		trades = append(trades, Trade{
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

// writeCheckpoint writes the checkpoint item to the database
func (dbw *DatabaseWriter) writeCheckpoint() {
	delta := Delta{
		Timestamp: float64(time.Now().UnixNano()),
		Price:     0, // price zero indicates the checkpoint
		Amount:    0,
	}
	fmt.Println("writing checkpoint", dbw.MarketDescriptor, time.Now())
	dbw.writeDeltas(delta)
}


// extractDeltas serializes the OrderBookDelta update to a list of Items
func (ob *OrderBook) extractDeltas(obd common.OrderBookDelta) []Delta {
	var deltas []Delta

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
		deltas = append(deltas, Delta{
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
		deltas = append(deltas, Delta{
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

func (ob *OrderBook) ApplyTrades(update common.TradesUpdate) {
	trades := ob.extractTrades(update)
	ob.intervalTrades = append(ob.intervalTrades, trades...)
}

func (ob *OrderBook) clearSnapshotData() {
	ob.intervalTrades = nil
	ob.intervalDeltas = nil
}
