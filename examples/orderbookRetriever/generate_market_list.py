import json
from itertools import groupby
import urllib.request

with urllib.request.urlopen("https://api.cryptowat.ch/markets") as url:
    markets = json.loads(url.read().decode())["result"]


exchanges = ["bitfinex", "binance", "kraken", "bitstamp", "bittrex", "coinbase-pro", "bitmex"]

quotes = ["usd", "usdt", "btc", "eth"]

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

