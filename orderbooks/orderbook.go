package orderbooks

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
	"fmt"
	"github.com/juju/errors"
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
	snapshot           common.OrderBookSnapshot
	marketDescriptor   rest.MarketDescr
	lastCheckpoint     time.Time
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

func Filter(vs []string, f func(string) bool) []string {
	vsf := make([]string, 0)
	for _, v := range vs {
		if f(v) {
			vsf = append(vsf, v)
		}
	}
	return vsf
}

func FilterSets(vs []common.PublicOrder, f func(common.PublicOrder) bool) []common.PublicOrder {
	vsf := make([]common.PublicOrder, 0)
	for _, v := range vs {
		if f(v) {
			vsf = append(vsf, v)
		}
	}
	return vsf
}

func (ob *OrderBook) applyObdRemovalsSide(obd *common.OrderBookDelta){
	bidPrice, _ := strconv.ParseFloat(ob.snapshot.Bids[0].Price, 64);
	askPrice, _ := strconv.ParseFloat(ob.snapshot.Asks[0].Price, 64);


	for _, newAsk := range(obd.Asks.Set){
		newPrice, _ := strconv.ParseFloat(newAsk.Price, 64);
		if newPrice<=bidPrice{
			for _, bid := range(ob.snapshot.Bids){
				p, _ := strconv.ParseFloat(bid.Price, 64)
				if newPrice<=p{
					obd.Bids.Remove = append(obd.Bids.Remove, bid.Price)
					//fmt.Println(newAsk.Price, "removing bid", bid.Price)
				}else{
					break
				}
			}
			//fmt.Println()
		}
	}

	for _, newBid := range(obd.Bids.Set){
		newPrice, _ := strconv.ParseFloat(newBid.Price, 64);
		if newPrice>=askPrice{
			for _, ask := range(ob.snapshot.Asks){
				p, _ := strconv.ParseFloat(ask.Price, 64)
				if newPrice>=p{
					obd.Asks.Remove = append(obd.Asks.Remove, ask.Price)
					//fmt.Println(newBid.Price, "removing ask", ask.Price)
				}else{
					break
				}
			}
			//fmt.Println()
		}
	}

	obd.Bids.Remove = Filter(obd.Bids.Remove, func(s string) bool {
		price, _ := strconv.ParseFloat(s, 64);
		if price>bidPrice{
			//fmt.Println("removing bid removal", price)
			return false
		}
		return true
	})

	obd.Asks.Remove = Filter(obd.Asks.Remove, func(s string) bool {
		price, _ := strconv.ParseFloat(s, 64);
		if price<askPrice{
			//fmt.Println("removing ask removal", price)
			return false
		}
		return true
	})



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
		ob.SetSnapshotCheckpoint(writer, obd.Timestamp)
		//fmt.Println(obd.Timestamp, "empty")
		return nil
	}

	if len( ob.snapshot.Asks)>0 && len(ob.snapshot.Bids)>0{
		ob.applyObdRemovalsSide(&obd)
	}

	ob.snapshot.Bids = ordersWithDelta(ob.snapshot.Bids, &obd.Bids, true)
	ob.snapshot.Asks = ordersWithDelta(ob.snapshot.Asks, &obd.Asks, false)

	if len( ob.snapshot.Asks)>0 && len(ob.snapshot.Bids)>0{
		bidPrice, _ := strconv.ParseFloat(ob.snapshot.Bids[0].Price, 64)
		askPrice, _ := strconv.ParseFloat(ob.snapshot.Asks[0].Price, 64)
		if bidPrice>askPrice{
			fmt.Println("negative", ob.snapshot.Asks[0].Price, ob.snapshot.Bids[0].Price , obd.Timestamp, obd)

			for _, bid := range(ob.snapshot.Bids){
				b, _ := strconv.ParseFloat(bid.Price, 64)
				if b>askPrice{
					obd.Bids.Remove = append(obd.Bids.Remove, bid.Price)
					fmt.Println("removing bid", bid.Price)
				}else{
					break
				}
			}

			for _, ask := range(ob.snapshot.Asks){
				a, _ := strconv.ParseFloat(ask.Price, 64)
				if a<bidPrice{
					obd.Asks.Remove = append(obd.Asks.Remove, ask.Price)
					fmt.Println("removing ask", ask.Price)
				}else{
					break
				}
			}
			ob.snapshot.Bids = ordersWithDelta(ob.snapshot.Bids, &obd.Bids, true)
			ob.snapshot.Asks = ordersWithDelta(ob.snapshot.Asks, &obd.Asks, false)

			bidPrice, _ := strconv.ParseFloat(ob.snapshot.Bids[0].Price, 64)
			askPrice, _ := strconv.ParseFloat(ob.snapshot.Asks[0].Price, 64)
			if bidPrice>askPrice{
				fmt.Println("negative again", ob.snapshot.Asks[0].Price, ob.snapshot.Bids[0].Price , obd.Timestamp, obd)
			}

		}
	}

	if writer != nil {
		//fmt.Println(obd.Timestamp)
		writer.writeDelta(obd)
	}


	ob.snapshot.SeqNum = obd.SeqNum

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
func (ob *OrderBook) SetSnapshotCheckpoint(writer *DatabaseWriter, ts time.Time) {
	if writer != nil {
		writer.writeCheckpoint(ts)
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

	toRemove := map[string]bool{}


	// Add new orders (which are still in setMap)
	for _, order := range setMap {
		if _, ok := removeMap[order.Price]; ok && reverse==true{
			// Need to remove this order, so don't add it
			//fmt.Println(order.Price)
			toRemove[order.Price] = true
			continue
		}
		newOrders = append(newOrders, order)
	}

	//fmt.Println(deltas.Set)
	deltas.Set = FilterSets(deltas.Set, func(o common.PublicOrder) bool {
		if _, ok := toRemove[o.Price]; ok {
			//fmt.Println("removing", o.Price, reverse)
			return false
		}
		return true
	})

	// Sort results
	if !reverse {
		sort.Sort(common.PublicOrdersByPrice(newOrders))
	} else {
		sort.Sort(sort.Reverse(common.PublicOrdersByPrice(newOrders)))
	}

	return newOrders
}
