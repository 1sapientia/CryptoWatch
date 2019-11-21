package orderbooks

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
	"fmt"
	"log"
	"math"
	"time"
)

type ArbitrageBacktester struct {
	Pair      rest.PairDescr
	Queue     SyncerQueue
}

// GetNextTimestamp returns the timestamp of the next update to be added to the window
func (ab *ArbitrageBacktester) Run()  {

}


type OrderbookSyncer struct {
	Market rest.MarketDescr
	FeePercentage float64
	OpportunityDurationFilter time.Duration
	chanOpen bool
	windowUpdated bool
	C chan common.OrderBookSnapshot
	nextSnapshot common.OrderBookSnapshot
	filteredSnapshot common.OrderBookSnapshot
	activeSnapshots []*common.OrderBookSnapshot
}

// GetNextTimestamp returns the timestamp of the next update to be added to the window
func (ob *OrderbookSyncer) GetNextTimestamp() time.Time {
	if ob.chanOpen{
		return ob.nextSnapshot.Timestamp
	}
	log.Fatal("channel is closed")
	return time.Now()
}

// BlockForFirstSnapshot blocks until the first snasphot is recieved from the database via channel.
func (ob *OrderbookSyncer) BlockForFirstSnapshot() {
	ob.nextSnapshot, ob.chanOpen = <-ob.C
	fmt.Println("unblocked")
}

// ConsumeNextSnapshot adds next snapshot to the active snapshots and updates the NextSnapshot with next channel item.
func (ob *OrderbookSyncer) ConsumeNextSnapshot() {
	tmp := ob.nextSnapshot
	ob.activeSnapshots = append(ob.activeSnapshots, &tmp)
	ob.recalculateFilteredSnapshot()
	ob.nextSnapshot, ob.chanOpen = <-ob.C
}

// GetFilteredSnapshot updates the filteredSnapshot if necessary and returns it.
func (ob *OrderbookSyncer) GetFilteredSnapshot(ts time.Time) common.OrderBookSnapshot {
	ob.removeExpiredSnapshots(ts)
	return ob.filteredSnapshot
}

// removeExpiredSnapshots the expired active snapshots and recalculates Filtered snapshot if needed.
func (ob *OrderbookSyncer) removeExpiredSnapshots(ts time.Time) {
	var cutoffIndex int
	for i, s := range ob.activeSnapshots{
		if ts.Sub(s.Timestamp) < ob.OpportunityDurationFilter{
			cutoffIndex = i
			break
		}
	}
	if cutoffIndex>0{
		ob.activeSnapshots = ob.activeSnapshots[cutoffIndex:]
		ob.recalculateFilteredSnapshot()
	}
}

// recalculateFilteredSnapshot calculates and saves current filtered snapshot from activeSnapshots
// it should be called whenever active snapshots are changed (new added or old removed).
func (ob *OrderbookSyncer) recalculateFilteredSnapshot() {
	maxAsk := 0.0
	minBid := math.MaxFloat64
	maxAskIndex := 0
	minBidIndex := 0
	for i, s := range ob.activeSnapshots{
		if s.Bid<minBid{
			minBid = s.Bid
			minBidIndex = i
		}
		if s.Ask>maxAsk{
			maxAsk = s.Ask
			maxAskIndex = i
		}
	}
	ob.filteredSnapshot.Asks = ob.activeSnapshots[maxAskIndex].Asks
	ob.filteredSnapshot.Ask = ob.activeSnapshots[maxAskIndex].Ask

	ob.filteredSnapshot.Bids = ob.activeSnapshots[minBidIndex].Bids
	ob.filteredSnapshot.Bid = ob.activeSnapshots[minBidIndex].Bid

}






