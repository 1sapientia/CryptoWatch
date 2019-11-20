package orderbooks

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
	"log"
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
}

// ConsumeNextSnapshot adds next snapshot to the active snapshots and updates the NextSnapshot with next channel item.
func (ob *OrderbookSyncer) ConsumeNextSnapshot() {
	ob.activeSnapshots = append(ob.activeSnapshots, &ob.nextSnapshot)
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
	ob.filteredSnapshot = *ob.activeSnapshots[0]
}






