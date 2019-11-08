import json
from itertools import groupby
import urllib.request

with urllib.request.urlopen("https://api.cryptowat.ch/markets") as url:
    markets = json.loads(url.read().decode())["result"]

with urllib.request.urlopen("https://api.cryptowat.ch/exchanges") as url:
    exchanges = json.loads(url.read().decode())["result"]

exchanges = {'gemini': 'Gemini',
     'bitfinex': 'Bitfinex',
     'gateio': 'Gate.io',
     'bitflyer': 'bitFlyer',
     'okex': 'Okex',
     'liquid': 'Liquid',
     'binance-us': 'Binance.US',
     'dex-aggregated': 'DEX (aggregated)',
     'kraken-futures': 'Kraken Futures',
     'huobi': 'Huobi',
     'coinone': 'Coinone',
     'coinbase-pro': 'Coinbase Pro',
     'bisq': 'Bisq',
     'bitbay': 'BitBay',
     'bithumb': 'Bithumb',
     'binance': 'Binance',
     'kraken': 'Kraken',
     'quoine': 'Quoine',
     'bitmex': 'BitMEX',
     'bittrex': 'Bittrex',
     'cexio': 'CEX.IO',
     'poloniex': 'Poloniex',
     'bitz': 'Bit-Z',
     'bitstamp': 'Bitstamp',
     'luno': 'Luno',
     'hitbtc': 'HitBTC',
     'okcoin': 'OKCoin'}

quotes = ["usd", "usdt", "btc", "eth", "usdc", "tust", "busd", "eurt", "cnht", "dai"]

filtered_markets = [market for market in markets
                    if market["exchange"] in exchanges
                    and market["active"]
                    and any(market["pair"].split("-")[0].endswith(q) for q in quotes)]


g = groupby(sorted(filtered_markets, key=lambda x: x["exchange"]), lambda x: x["exchange"])

dic = {}
for key, group in g:
    dic[key]=list(x for x in group)


print("exchange_pairs:")
for exchange, markets in dic.items():
    print("\n  "+exchange+":")
    for m in markets:
        print("    - \""+m["pair"]+"\"")

