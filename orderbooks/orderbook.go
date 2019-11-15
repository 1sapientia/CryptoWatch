package orderbooks

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
	"fmt"
	"github.com/juju/errors"
	"math"
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
	intervalTrades     []Item
	intervalDeltas     []Item
	snapshot           common.OrderBookSnapshot
	marketDescriptor   rest.MarketDescr
	lastCheckpoint     time.Time
	ExchangeDescriptor rest.ExchangeDescr
	PairDescriptor     rest.PairDescr
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

	if obd.SeqNum==999999999{
		ob.snapshot = common.OrderBookSnapshot{}
		fmt.Println("checkpoint")
		return nil
	}

	startTime, _ := time.Parse("2006-01-02 15:04:05.000", "2019-11-10 00:00:00.000")
	EndTime, _ := time.Parse("2006-01-02 15:04:05.000", "2019-11-10 23:00:00.000")

	if obd.Timestamp.Before(EndTime)&&obd.Timestamp.After(startTime){
		//fmt.Println(obd)
	}

	deltaItems := writer.extractDeltas(obd)

	if obd.Timestamp.Before(EndTime)&&obd.Timestamp.After(startTime){
		for _, item := range deltaItems{
			fmt.Printf(",[\"n\",%d,%f]",obd.Timestamp.UnixNano(), math.Abs(item.Price))
		}
	}


	ob.intervalDeltas = append(ob.intervalDeltas, deltaItems...)

	if writer.WriteToDB{
		writer.writeDelta(obd)
	}

	ob.snapshot.Bids = ordersWithDelta(ob.snapshot.Bids, &obd.Bids, true)
	ob.snapshot.Asks = ordersWithDelta(ob.snapshot.Asks, &obd.Asks, false)

	if len( ob.snapshot.Asks)<=0{
		//fmt.Println("wtf", obd.Timestamp)

	}
	//fmt.Println("wtf", obd.Timestamp)


	if len( ob.snapshot.Asks)>0 && len(ob.snapshot.Bids)>0{
		ask, _ := strconv.ParseFloat(ob.snapshot.Asks[0].Price, 64);
		bid, _ := strconv.ParseFloat(ob.snapshot.Bids[0].Price, 64);
		if ask<bid{
			fmt.Println(ob.snapshot.Bids[0].Price, ob.snapshot.Asks[0].Price , ob.marketDescriptor.Exchange, ob.marketDescriptor.Pair, obd.Timestamp, obd)
		}
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
	if writer.WriteToDB {
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
		newOrders = append(newOrders, order)
	}

	//Sort results
	if !reverse {
		sort.Sort(common.PublicOrdersByPrice(newOrders))
	} else {
		sort.Sort(sort.Reverse(common.PublicOrdersByPrice(newOrders)))
	}

	return newOrders
}


func (ob *OrderBook) ApplyTrades(update common.TradesUpdate, writer *DatabaseWriter) {
	trades := writer.extractTrades(update)
	ob.intervalTrades = append(ob.intervalTrades, trades...)
}

func (ob *OrderBook) clearSnapshotData() {
	ob.intervalTrades = nil
	ob.intervalDeltas = nil
}

