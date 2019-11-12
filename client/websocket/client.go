package websocket

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/common"
)

type Client interface {
	Connect() (err error)
	OnStateChange(state ConnState, cb StateCallback)
	OnMarketUpdate(cb MarketUpdateCB)
	Close() (err error)
}

// MarketUpdateCB defines a callback function for OnMarketUpdate.
type MarketUpdateCB func(common.Market, common.MarketUpdate)

type callMarketUpdateListenersReq struct {
	market    common.Market
	update    common.MarketUpdate
	listeners []MarketUpdateCB
}

type StreamSubscription struct {
	Resource           string
	ExchangeDescriptor rest.ExchangeDescr
	PairDescriptor     rest.PairDescr
}

func (s *StreamSubscription) GetResource() string {
	return s.Resource
}

// PairUpdateCB defines a callback function for OnPairUpdate.
type PairUpdateCB func(common.Pair, common.PairUpdate)

type callPairUpdateListenersReq struct {
	listeners []PairUpdateCB
	pair      common.Pair
	update    common.PairUpdate
}

type callSubscriptionResultListenersReq struct {
	listeners []SubscriptionResultCB
	result    SubscriptionResult
}

type callUnsubscriptionResultListenersReq struct {
	listeners []UnsubscriptionResultCB
	result    UnsubscriptionResult
}

type callBandwidthUpdateListenersReq struct {
	listeners []BandwidthUpdateCB
	msg       Bandwidth
}

type callMissedMessagesListenersReq struct {
	listeners []MissedMessagesCB
	msg       MissedMessages
}
