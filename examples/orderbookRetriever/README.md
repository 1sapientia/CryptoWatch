# Arbitrage Backtester

##oportunity evaluation measure

Per pair opportunity evaluation considering following parameters:

- **fees** - fees can be accounted for by simply skewing the orderbook (increasing the spread according to the expected fee)

- **oportunity sliding window** - many times the opportunities are merely the result of slightly out of sync exchange order book feeds
      and do not reflect reality. additionally latency and order execution problems can prevent us from successfully utilizing the mismatches.
      It is therefore important to make sure that the "high frequency" opportunities are evaluated conservatively.
      Sliding window can introduce a filter where highest frequency opportunities can be filtered out to avoid over-optimistic results.

- **oportunity utilization strategy** - how to evaluate the opportunity without disregarding our effect on the market.
    our gains are limited by our available assets which, when traded, also affect the orderbook. This means that although
    the opportunity seems to persist for a long period of time, we are only able to exploit it once. Additionally usually long term
    opportunities only exist due to some anomaly in the market (disabled withdrawals, technical issues, ...) and should be handled carefully

- **deployed assets** - oportunity utilization strategy above can potentially account for available assets.

###aggregated data for charts
Per pair orderbooks movements (how do we visualize available volume?)



