import json
from collections import defaultdict
from itertools import groupby
import urllib.request

import numpy as np

with urllib.request.urlopen("https://api.cryptowat.ch/markets") as url:
    markets = json.loads(url.read().decode())["result"]

with urllib.request.urlopen("https://api.cryptowat.ch/exchanges") as url:
    exchanges = json.loads(url.read().decode())["result"]

quotes = ["btc", "eth"]


top100cmc = ['BTC','ETH','XRP','BCH','USDT','LTC','EOS','BNB','BSV','XLM','TRX','ADA','XMR','LEO','LINK','HT','XTZ','NEO','ATOM','MIOTA','MKR','DASH','ETC','ONT','USDC','CRO','XEM','BAT','DOGE','ZEC','VET','PAX','DCR','HEDG','QTUM','ZRX','TUSD','HOT','BTG','CENNZ','RVN','ZB','OMG','VSYS','REP','NANO','ABBC','LUNA','BTM','SNX','ALGO','EKT','KCS','DAI','BTT','LSK','BCD','KMD','DGB','ICX','SC','HC','QNT','SXP','WAVES','BCN','BTS','KBC','THETA','IOST','MONA','FTT','MCO','DX','AE','MAID','XVG','SEELE','AOA','NEXO','NRG','ARDR','CHZ','ZIL','STEEM','ENJ','RLC','RIF','GNT','ELF','SNT','REN','NPXS','ILC','CRPT','XZC','NEW','HPT','ODE','SOLVE']
top100cw = ['BTC','ETH','XRP','EOS','LTC','BCH','TUSD','PAX','QTUM','TRX','BNB','BSV','ETC','LINK','NKN','XLM','XMR','KAVA','ATOM','NEO','VET','BZ','SEELE','ONT','XTZ','IOST','ADA','LAMB','ZEC','USDT','USDC','ARPA','BAT','BTM','MATIC','ALGO','ABBC','FET','DASH','DOGE','ZRX','REP','BUSD','STX','DAI','CHZ','EKT','IOTA','OMG','GXC','NEXO','RVN','ZIL','ICX','CDT','NAS','GNT','EGT','HBAR','MTL','XEM','BCV','WTC','NANO','BEAM','NULS','WAVES','WIN','BAND','DGTX','BTG','HPT','WXT','ETP','KMD','HC','MX','KEY','DCR','ONE','BHD','LXT','MDA','ERD','WAN','AE','COS','PERL','REN','GT','AKRO','HOT','TOMO','FTM','FTT','TNT','USDS','DOCK','THETA','BLZ']
top100 = [c.lower() for c in top100cw]
top100.extend(quotes)


exchanges = {
     'bitfinex': 'Bitfinex',
     'binance': 'Binance',
     'kraken': 'Kraken',
     'bittrex': 'Bittrex',
     'bitstamp': 'Bitstamp',
    }

def get_quote(market):
    quote_id = np.argmax([market.endswith(q) for q in quotes])
    return quotes[quote_id]

def get_base(market):
    quote_id = np.argmax([market.endswith(q) for q in quotes])
    base = market[:-len(quotes[quote_id])]
    return base

def get_market(market_with_futures):
    return market_with_futures.split("-")[0]

filtered_markets = [market for market in markets
                    if market["exchange"] in exchanges
                    and market["active"]
                    and any(get_market(market['pair']).endswith(q) for q in quotes)]

only_top100 = [market for market in filtered_markets if get_base(get_market(market['pair'])) in top100]

only_top100_coins = [get_base(get_market(market['pair'])) for market in filtered_markets if get_base(get_market(market['pair'])) in top100]

#len(only_top100)
#len(set(only_top100_coins))

g = groupby(sorted(only_top100, key=lambda x: x["exchange"]), lambda x: x["exchange"])

dic = {}
dic1 = {}
for key, group in g:
    dic[key]=list(x for x in group)


cntr = defaultdict(int)
for exchange, markets in dic.items():
    for m in markets:
        cntr[m["pair"]]+=1


print("exchange_pairs:")
for exchange, markets in dic.items():
    print("\n  "+exchange+":")
    for m in markets:
        if cntr[m["pair"]]>1:
            print("    - \""+m["pair"]+"\"")

