/*
Package common provides data structures used throughout the Cryptowatch SDK.
*/
package common // import "code.cryptowat.ch/cw-sdk-go/common"
import (
	"fmt"
	"strings"
)

//fixExchangeName maps the cryptowatch exchange names to correct format (camelcase)
func FixExchangeName(old string) string {
	mapping := map[string]string{
		"bitfinex":     "Bitfinex",
		"binance":      "Binance",
		"kraken":       "Kraken",
		"bitstamp":     "Bitstamp",
		"bittrex":      "Bittrex",
		"coinbase-pro": "CoinbasePro",
		"bitmex":       "Bitmex",
	}
	if val, ok := mapping[old]; ok {
		return val
	}
	fmt.Println("Warning: exchange mapping not defined. using default", old)
	return strings.Title(strings.ToLower(old))
}

//fixPair maps the cryptowatch pair names to correct format (BASE/QUOTE/potential[Q,P,W] for futures)
func FixPair(old string) string {
	s := strings.Split(old, "-")
	pair := s[0]
	var quote string
	if strings.HasSuffix(pair, "usd") {
		quote = "USD"
	} else if strings.HasSuffix(pair, "usdt") {
		quote = "USDT"
	} else if strings.HasSuffix(pair, "btc") {
		quote = "BTC"
	} else if strings.HasSuffix(pair, "eth") {
		quote = "eth"
	} else {
		fmt.Println("Warning: pair mapping not defined. using default", old)
		quote = pair[len(pair)-3:]
	}
	base := pair[:len(pair)-len(quote)]
	newPair := base + "/" + quote
	if len(s) > 1 {
		future := s[1][0:1]
		newPair += "/" + future
	}
	return strings.ToUpper(newPair)
}