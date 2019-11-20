package orderbooks

import (
	"container/heap"
	"fmt"
	"time"
)

// A SyncerQueue implements heap.Interface and holds OrderbookSyncers prioritized by timestamp (lowest first)
type SyncerQueue []*OrderbookSyncer

func (pq SyncerQueue) Len() int { return len(pq) }

func (pq SyncerQueue) Less(i, j int) bool {
	return pq[i].GetNextTimestamp().Before(pq[j].GetNextTimestamp())
}

func (pq SyncerQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *SyncerQueue) Push(x interface{}) {
	item := x.(*OrderbookSyncer)
	*pq = append(*pq, item)
}

func (pq *SyncerQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

// updateNext consumes the top priority OrderbookSyncer snapshot which updates its priority and changes its position in the priority queue
// it returns the timestamp of the consumed snapshot which represents the current synced timestamp
func (pq *SyncerQueue) updateNext() time.Time{
	ts := (*pq)[pq.Len()-1].GetNextTimestamp()
	(*pq)[pq.Len()-1].ConsumeNextSnapshot()
	heap.Fix(pq, pq.Len()-1)
	return ts
}

// update consumes the top priority OrderbookSyncer snapshot which updates its priority and changes its position in the priority queue
func (pq *SyncerQueue) RunBacktest() {
	for _, syncer := range *pq{
		fmt.Println("blocking for ", syncer.Market)
		syncer.BlockForFirstSnapshot()
	}
	for{
		if pq.Len() == 0{
			fmt.Println("backtest done")
			break
		}
		ts := pq.updateNext()
		pq.evaluateOpportunities(ts)
	}
}

// evaluateOpportunities for now only prints out the filtered snapshots
func (pq *SyncerQueue) evaluateOpportunities(ts time.Time) {
	for _, syncer := range *pq{
		fmt.Println(syncer.Market, syncer.GetFilteredSnapshot(ts))
	}
}



