package orderbooks

import (
	"code.cryptowat.ch/cw-sdk-go/common"
	"container/heap"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
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
	ts := (*pq)[0].GetNextTimestamp()

	//fmt.Println((*pq)[0].Market, ts)

	(*pq)[0].ConsumeNextSnapshot()
	heap.Fix(pq, 0)
	return ts
}

// update consumes the top priority OrderbookSyncer snapshot which updates its priority and changes its position in the priority queue
func (pq *SyncerQueue) RunBacktest(symbol string) {

	f, err := os.OpenFile(symbol+".json", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()


	maxBid := 0.0
	minAsk := math.MaxFloat64
	overlapping := common.OrderBookSnapshot{
		Bids:      []common.PublicOrder{},
		Asks:      []common.PublicOrder{},
		Bid:       maxBid,
		Ask:	   minAsk,
	}
	for _, syncer := range *pq{
		fmt.Println("blocking for ", syncer.Market)
		syncer.BlockForFirstSnapshot()
	}
	heap.Init(pq)
	for{
		if pq.Len() == 0{
			fmt.Println("backtest done")
			break
		}
		ts := pq.updateNext()
		pq.evaluateOpportunities(ts, &overlapping, f)
	}
}

// evaluateOpportunities for now only prints out the filtered snapshots
func (pq *SyncerQueue) evaluateOpportunities(ts time.Time, overlapping *common.OrderBookSnapshot, file *os.File) {
	maxBid := 0.0
	minAsk := math.MaxFloat64
	for _, syncer := range *pq{
		maxBid = math.Max(maxBid, syncer.GetFilteredSnapshot(ts).Bid)
		minAsk = math.Min(minAsk, syncer.GetFilteredSnapshot(ts).Ask)
	}
	if(maxBid > minAsk && maxBid/minAsk-1>0.001 && (maxBid != overlapping.Bid || minAsk != overlapping.Ask)){
		overlapping.Timestamp = ts
		overlapping.Asks = []common.PublicOrder{}
		overlapping.Bids = []common.PublicOrder{}
		overlapping.Bid = maxBid
		overlapping.Ask = minAsk
		for _, syncer := range *pq{
			for _, order := range syncer.GetFilteredSnapshot(ts).Bids{
				p, _ := strconv.ParseFloat(order.Price, 64)
				if p < minAsk{
					break
				}
				order.Exchange = syncer.Market.Exchange
				overlapping.Bids = append(overlapping.Bids, order)
			}
			for _, order := range syncer.GetFilteredSnapshot(ts).Asks{
				p, _ := strconv.ParseFloat(order.Price, 64)
				if p > maxBid{
					break
				}
				order.Exchange = syncer.Market.Exchange
				overlapping.Asks = append(overlapping.Asks, order)
			}
		}
		sort.Sort(common.PublicOrdersByPrice(overlapping.Asks))
		sort.Sort(sort.Reverse(common.PublicOrdersByPrice(overlapping.Bids)))
		e, _ := json.Marshal(*overlapping)

		if _, err := file.WriteString(string(e)+",\n"); err != nil {
			log.Println(err)
		}
		//fmt.Println(string(e)+",")
	}
}



//17:15