package main

import (
	"code.cryptowat.ch/cw-sdk-go/client/rest"
	"code.cryptowat.ch/cw-sdk-go/client/websocket"
	"code.cryptowat.ch/cw-sdk-go/common"
	"code.cryptowat.ch/cw-sdk-go/config"
	"code.cryptowat.ch/cw-sdk-go/orderbooks"
	"fmt"
	flag "github.com/spf13/pflag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const (
	defaultExchange = "bitfinex"
	defaultPair     = "btcusd"
)

func main() {

	// We need this since getting user's home dir can fail.
	defaultConfig, err := config.DefaultFilepath()
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}

	var (
		configFile string
		verbose    bool
	)

	flag.StringVarP(&configFile, "config", "c", defaultConfig, "Configuration file")
	flag.BoolVarP(&verbose, "verbose", "v", false, "Prints all debug messages to stdout")

	flag.Parse()

	cfg, err := config.New(configFile)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		log.Print(err)
		os.Exit(1)
	}

	restclient := rest.NewCWRESTClient(nil)

	// Get market descriptions and match it with market IDs
	markets := map[int]rest.MarketDescr{}
	for exchange, pairs := range cfg.ExchangePairs {
		for _, pair := range pairs {
			market, err := restclient.GetMarketDescr(exchange, pair)
			if err != nil {
				log.Printf("failed to get market %s/%s: %s", exchange, pair, err)
				os.Exit(1)
			}
			fmt.Println(exchange, pair, market)
			markets[market.ID] = market
		}
	}

	subscriptions := []*websocket.StreamSubscription{}
	orderbookUpdaters := map[int64]*orderbooks.OrderBookUpdater{}

	// generate subscriptions and orderBookUpdaters for every market
	for _, market := range markets {
		subscriptions = append(subscriptions,
			&websocket.StreamSubscription{
				Resource: fmt.Sprintf("markets:%d:book:deltas", market.ID),
			},
			&websocket.StreamSubscription{
				Resource: fmt.Sprintf("markets:%d:book:snapshots", market.ID),
			},
			&websocket.StreamSubscription{
				Resource: fmt.Sprintf("markets:%d:trades", market.ID),
			})

		orderbookUpdater := orderbooks.NewOrderBookUpdater(&orderbooks.OrderBookUpdaterParams{
			WriteToDB:          true,
			OrderbookTableName: "Orderbooks",
			TradesTableName:    "Trades",
			MarketDescriptor:   market,
			SnapshotGetter: orderbooks.NewOrderBookSnapshotGetterRESTBySymbol(
				market.Exchange, market.Pair, &rest.CWRESTClientParams{
					APIURL: cfg.APIURL,
				},
			),
		})
		orderbookUpdaters[int64(market.ID)] = orderbookUpdater
	}

	// Create a new stream connection instance
	c, err := websocket.NewStreamClient(&websocket.StreamClientParams{
		WSParams: &websocket.WSParams{
			URL:       cfg.StreamURL,
			APIKey:    cfg.APIKey,
			SecretKey: cfg.SecretKey,
		},
		Subscriptions: subscriptions,
	})
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}

	// Ask for the state transition updates and print them.
	c.OnStateChange(
		websocket.ConnStateAny,
		func(oldState, state websocket.ConnState) {
			log.Printf(
				"State updated: %s -> %s",
				websocket.ConnStateNames[oldState],
				websocket.ConnStateNames[state],
			)
		},
	)

	// Listen for market changes.
	c.OnMarketUpdate(
		func(market common.Market, md common.MarketUpdate) {
			marketID, _ := market.ID.Int64()
			//fmt.Println(market)
			if snapshot := md.OrderBookSnapshot; snapshot != nil {
				orderbookUpdaters[marketID].ReceiveSnapshot(*snapshot)
			} else if delta := md.OrderBookDelta; delta != nil {
				orderbookUpdaters[marketID].ReceiveDelta(*delta)
			} else if trades := md.TradesUpdate; trades != nil {
				orderbookUpdaters[marketID].ReceiveTrades(*trades)
			}
		},
	)

	// Finally, connect.
	if err := c.Connect(); err != nil {
		log.Print(err)
		os.Exit(1)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// Wait until the OS signal is received, at which point we'll close the connection and quit.
	<-signals

	log.Print("Closing connection...")

	if err := c.Close(); err != nil {
		log.Printf("Failed to close connection: %s", err)
	}
}
