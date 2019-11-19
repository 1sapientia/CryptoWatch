package orderbooks

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
	"log"
	"time"
)

type ArbitrageBacktester struct {
	pair rest.PairDescr
	exchanges map[string]OrderbookSyncer
}



// Run synchronously merges data from different OrderbookSyncers and evaluates the arbitrage opportunities
func (ab *ArbitrageBacktester) Run(){
	for{
		if len(ab.exchanges) == 0{
			break
		}
	}
}



type OrderbookSyncer struct {
	market rest.MarketDescr
	feePercentage float64
	opportunityDurationFilter time.Duration
	chanOpen bool
	windowUpdated bool
	c chan common.OrderBookSnapshot
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

// ConsumeNextSnapshot adds next snapshot to the active snapshots and updates the NextSnapshot with next channel item.
func (ob *OrderbookSyncer) ConsumeNextSnapshot() {
	ob.activeSnapshots = append(ob.activeSnapshots, &ob.nextSnapshot)
	ob.recalculateFilteredSnapshot()
	ob.nextSnapshot, ob.chanOpen = <-ob.c
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
		if ts.Sub(s.Timestamp) < ob.opportunityDurationFilter{
			cutoffIndex = i
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

}






