# Binance Data Fetcher
Replicates many of the features from [gdaxfeeder] (https://github.com/alpacahq/marketstore/tree/master/contrib/gdaxfeeder)
This module builds a MarketStore background worker which fetches historical
price data of cryptocurrencies from Binance's public API.  It runs as a goroutine
behind the MarketStore process and keeps writing to the disk.

## Configuration
binancefeeder.so comes with the server by default, so you can simply configure it
in MarketStore configuration file.

### Options
Name | Type | Default | Description
--- | --- | --- | ---
query_start | string | none | The point in time from which to start fetching price data
query_end | string | none | The point in time from which to end fetching price data. If not set, the binancefeeder will forever grab data. If set, it will end on query_end and continually retrieve "query_end" to "query_end" data since this is a background worker and will forver run in the background.
base_currency | string | USDT | Base currency for symbols. ex: BTC, ETH, USDT
base_timeframes | slice of string | ["1Min", "1H", "1D"] | The bar aggregation durations
symbols | slice of strings | [All "trading" symbols from https://api.binance.com/api/v1/exchangeInfo] | The symbols to retrieve data for

#### Query Start
The fetcher keeps filling data up to the current time eventually and writes new data as it is
generated. It writes data every 30 * your time interval. It then pauses for 1 second after each call. Note that the data fetch timestamp is identical among symbols, so if one symbol lags other fetches may not be
up to speed.

#### Base Timeframe
The daily bars are written at the boundary of system timezone configured in the same file.

### Example
Add the following to your config file:
```
bgworkers:
  - module: binancefeeder.so
    name: BinanceFetcher
    config:
      symbols:
        - ETH
      base_timeframes:
        - 1Min
        - 1H
      base_currency: "USDT"
      query_start: "2018-01-01 00:00"
      query_end: "2018-01-02 00:00"
```


## Build
If you need to change the fetcher, you can build it by:

```
$ make configure
$ make all
```

It installs the new .so file to the first GOPATH/bin directory.


## Caveat
Since this is implemented based on the Go's plugin mechanism, it is supported only
on Linux & MacOS as of Go 1.10
