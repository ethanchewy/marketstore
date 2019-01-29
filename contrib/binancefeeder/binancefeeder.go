package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	binance "github.com/adshao/go-binance"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
)

var suffixBinanceDefs = map[string]string{
	"Min": "m",
	"H":   "h",
	"D":   "d",
	"W":   "w",
}

// ExchangeInfo exchange info
type ExchangeInfo struct {
	Timezone   string `json:"timezone"`
	ServerTime int64  `json:"serverTime"`
	RateLimits []struct {
		RateLimitType string `json:"rateLimitType"`
		Interval      string `json:"interval"`
		Limit         int    `json:"limit"`
	} `json:"rateLimits"`
	ExchangeFilters []interface{} `json:"exchangeFilters"`
	Symbols         []struct {
		Symbol             string   `json:"symbol"`
		Status             string   `json:"status"`
		BaseAsset          string   `json:"baseAsset"`
		BaseAssetPrecision int      `json:"baseAssetPrecision"`
		QuoteAsset         string   `json:"quoteAsset"`
		QuotePrecision     int      `json:"quotePrecision"`
		OrderTypes         []string `json:"orderTypes"`
		IcebergAllowed     bool     `json:"icebergAllowed"`
		Filters            []struct {
			FilterType       string `json:"filterType"`
			MinPrice         string `json:"minPrice,omitempty"`
			MaxPrice         string `json:"maxPrice,omitempty"`
			TickSize         string `json:"tickSize,omitempty"`
			MinQty           string `json:"minQty,omitempty"`
			MaxQty           string `json:"maxQty,omitempty"`
			StepSize         string `json:"stepSize,omitempty"`
			MinNotional      string `json:"minNotional,omitempty"`
			Limit            int    `json:"limit,omitempty"`
			MaxNumAlgoOrders int    `json:"maxNumAlgoOrders,omitempty"`
		} `json:"filters"`
	} `json:"symbols"`
}

// Get JSON via http request and decodes it using NewDecoder. Sets target interface to decoded json
func getJson(url string, target interface{}) error {
	var myClient = &http.Client{Timeout: 10 * time.Second}
	r, err := myClient.Get(url)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	return json.NewDecoder(r.Body).Decode(target)
}

// For ConvertStringToFloat function and Run() function to making exiting easier
var errorsConversion []error

// FetcherConfig is a structure of binancefeeder's parameters
type FetcherConfig struct {
	Symbols       []string `json:"symbols"`
	BaseCurrency  string   `json:"base_currency"`
	QueryStart    string   `json:"query_start"`
	BaseTimeframe string   `json:"base_timeframe"`
}

// BinanceFetcher is the main worker for Binance
type BinanceFetcher struct {
	config        map[string]interface{}
	symbols       []string
	baseCurrency  string
	queryStart    time.Time
	baseTimeframe *utils.Timeframe
}

// recast changes parsed JSON-encoded data represented as an interface to FetcherConfig structure
func recast(config map[string]interface{}) *FetcherConfig {
	data, _ := json.Marshal(config)
	ret := FetcherConfig{}
	json.Unmarshal(data, &ret)

	return &ret
}

// Check if slice has parity
// what type is tbk????
// func checkSliceParity(slice []time.Time{}) bool{
//
// }

//Convert string to float64 using strconv
func convertStringToFloat(str string) float64 {
	convertedString, err := strconv.ParseFloat(str, 64)
	//Store error in string array which will be checked in main fucntion later to see if there is a need to exit
	if err != nil {
		fmt.Printf("String to float error: %v\n", err)
		errorsConversion = append(errorsConversion, err)
	}
	return convertedString
}

//Checks time string and returns correct time format
func queryTime(query string) time.Time {
	trials := []string{
		"2006-01-02 03:04:05",
		"2006-01-02T03:04:05",
		"2006-01-02 03:04",
		"2006-01-02T03:04",
		"2006-01-02",
	}
	for _, layout := range trials {
		qs, err := time.Parse(layout, query)
		if err == nil {
			//Returns time in correct time.Time object once it matches correct time format
			return qs.In(utils.InstanceConfig.Timezone)
		}
	}
	//Return null if no time matches time format
	return time.Time{}
}

//Convert time from milliseconds to Unix
func convertMillToTime(originalTime int64) time.Time {
	i := time.Unix(0, originalTime*int64(time.Millisecond))
	return i
}

// Append if String is Missing from array
// All credit to Sonia: https://stackoverflow.com/questions/9251234/go-append-if-unique
func appendIfMissing(slice []string, i string) ([]string, bool) {
	for _, ele := range slice {
		if ele == i {
			return slice, false
		}
	}
	return append(slice, i), true
}

//Gets all symbols from binance
func getAllSymbols(quoteAsset string) []string {
	client := binance.NewClient("", "")
	m := ExchangeInfo{}
	err := getJson("https://api.binance.com/api/v1/exchangeInfo", &m)
	symbol := make([]string, 0)
	status := make([]string, 0)
	validSymbols := make([]string, 0)
	tradingSymbols := make([]string, 0)
	quote := ""

	if err != nil {
		fmt.Printf("Binance /exchangeInfo API error: %v\n", err)
		tradingSymbols = []string{"BTC", "EOS", "ETH", "BNB", "TRX", "ONT", "XRP", "ADA",
			"LTC", "BCC", "TUSD", "IOTA", "ETC", "ICX", "NEO", "XLM", "QTUM"}
	} else {
		for _, info := range m.Symbols {
			quote = info.QuoteAsset
			notRepeated := true
			// Check if data is the right base currency and then check if it's already recorded
			if quote == quoteAsset {
				symbol, notRepeated = appendIfMissing(symbol, info.BaseAsset)
				if notRepeated {
					status = append(status, info.Status)
				}
			}
		}

		//Check status and append to symbols list if valid
		for index, s := range status {
			if s == "TRADING" {
				tradingSymbols = append(tradingSymbols, symbol[index])
			}
		}
	}

	// Double check each symbol is working as intended
	for _, s := range tradingSymbols {
		_, err := client.NewKlinesService().Symbol(s + quoteAsset).Interval("1m").Do(context.Background())
		if err == nil {
			validSymbols = append(validSymbols, s)
		}
	}

	return validSymbols
}

// Find last timestamp for each cryptocurrency.
// If there is a new cryptocurrency, backfill from that timestamp => store in array?
// func findLastTimestamp(symbol string, tbk *io.TimeBucketKey) time.Time {
// 	cDir := executor.ThisInstance.CatalogDir
// 	query := planner.NewQuery(cDir)
// 	query.AddTargetKey(tbk)
// 	start := time.Unix(0, 0).In(utils.InstanceConfig.Timezone)
// 	end := time.Unix(math.MaxInt64, 0).In(utils.InstanceConfig.Timezone)
// 	query.SetRange(start.Unix(), end.Unix())
// 	query.SetRowLimit(io.LAST, 1)
// 	parsed, err := query.Parse()
// 	fmt.Println(parsed)
// 	if err != nil {
// 		return time.Time{}
// 	}
// 	reader, err := executor.NewReader(parsed)
// 	csm, err := reader.Read()
// 	cs := csm[*tbk]
// 	fmt.Println("TESTING TYPE FINDLASTTIMESTAMP")
// 	fmt.Println("cs %v", reflect.TypeOf(cs))
//
// 	if cs == nil || cs.Len() == 0 {
// 		return time.Time{}
// 	}
// 	ts := cs.GetTime()
// 	fmt.Println("ts %v", reflect.TypeOf(ts))
// 	fmt.Println("ts value: %v", ts)
//
// 	// See if there are any differences in time between time objects
// 	// IF time is different, that means that Binance recently updated and that there is a time object of 0 for new cryptocurrencies
//
// 	return ts[0]
// }

func findLastTimestamp(symbol string, tbk *io.TimeBucketKey) time.Time {
	cDir := executor.ThisInstance.CatalogDir
	query := planner.NewQuery(cDir)
	query.AddTargetKey(tbk)
	start := time.Unix(0, 0).In(utils.InstanceConfig.Timezone)
	end := time.Unix(math.MaxInt64, 0).In(utils.InstanceConfig.Timezone)
	query.SetRange(start.Unix(), end.Unix())
	query.SetRowLimit(io.LAST, 1)
	parsed, err := query.Parse()
	if err != nil {
		return time.Time{}
	}
	reader, err := executor.NewReader(parsed)
	csm, err := reader.Read()
	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return time.Time{}
	}
	ts := cs.GetTime()
	return ts[0]
}

// NewBgWorker registers a new background worker
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	config := recast(conf)
	var queryStart time.Time
	timeframeStr := "1Min"
	var symbols []string
	baseCurrency := "USDT"

	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}

	if config.BaseCurrency != "" {
		baseCurrency = config.BaseCurrency
	}

	if config.QueryStart != "" {
		queryStart = queryTime(config.QueryStart)
	}

	//First see if config has symbols, if not retrieve all from binance as default
	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	} else {
		symbols = getAllSymbols(baseCurrency)
	}

	return &BinanceFetcher{
		config:        conf,
		baseCurrency:  baseCurrency,
		symbols:       symbols,
		queryStart:    queryStart,
		baseTimeframe: utils.NewTimeframe(timeframeStr),
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (bn *BinanceFetcher) Run() {
	fmt.Println("testing123")
	symbols := bn.symbols
	client := binance.NewClient("", "")
	// timeStart := []time.Time{}
	baseCurrency := bn.baseCurrency
	slowDown := false

	// Get correct Time Interval for Binance
	originalInterval := bn.baseTimeframe.String
	re := regexp.MustCompile("[0-9]+")
	re2 := regexp.MustCompile("[a-zA-Z]+")
	timeIntervalLettersOnly := re.ReplaceAllString(originalInterval, "")
	timeIntervalNumsOnly := re2.ReplaceAllString(originalInterval, "")
	correctIntervalSymbol := suffixBinanceDefs[timeIntervalLettersOnly]
	if len(correctIntervalSymbol) <= 0 {
		fmt.Printf("Interval Symbol Format Incorrect. Setting to time interval to default '1Min'\n")
		correctIntervalSymbol = "1Min"
	}
	timeInterval := timeIntervalNumsOnly + correctIntervalSymbol

	isThereStartTime := false
	if !bn.queryStart.IsZero() {
		isThereStartTime = true
	}
	// Get last timestamp collected
	// Push that to the map of timeStarts
	var timeStarts = make(map[string]time.Time)
	for _, symbol := range symbols {
		tbk := io.NewTimeBucketKey(symbol + "/" + bn.baseTimeframe.String + "/OHLCV")
		lastTimestamp := findLastTimestamp(symbol, tbk)
		fmt.Printf("lastTimestamp for %s = %v\n", symbol, lastTimestamp)
		if timeStarts[symbol].IsZero() || (!lastTimestamp.IsZero() && lastTimestamp.Before(timeStarts[symbol])) {
			// timeStart = lastTimestamp
			timeStarts[symbol] = lastTimestamp
		} else {
			if isThereStartTime {
				timeStarts[symbol] = bn.queryStart
			} else {
				// Default value for timeStart for each symbol if not set is: start current time minus duration
				timeStarts[symbol] = time.Now().UTC().Add(-bn.baseTimeframe.Duration)
			}
		}
	}

	// Calculate which timestamps are new
	// Maybe not necesary though because you can just parse
	// var newTimeStarts =
	// Parse through every time stamp in next for loop and associate it with the timeStarts and timeEndM
	// MAP TIMESTART AND TIMEEND AHHHHHHHHHH

	// For loop for collecting candlestick data forever
	// Note that the max amount is 1000 candlesticks which is no problem
	var timeStartM int64
	var timeEndM int64
	var timeEnds = make(map[string]time.Time)
	var originalTimeStarts = make(map[string]time.Time)
	var originalTimeEnds = make(map[string]time.Time)
	var originalTimeEndZeros = make(map[string]time.Time)
	var waitTills = make(map[string]time.Time)
	firstLoop := true

	var multiplier time.Duration = 394200

	updating := false

	// Current iteration (symbol index)
	// var iteration = 0

	// yikes kinda looks messy smh
	for {
		// finalTime = time.Now().UTC()
		for _, symbol := range symbols {

			originalTimeStarts[symbol] = timeStarts[symbol]
			originalTimeEnds[symbol] = timeEnds[symbol]

			// Check if it's finished backfilling. If not, just do 600 * Timeframe.duration
			// only do beyond 1st loop
			if !slowDown {

				if !firstLoop {
					timeStarts[symbol] = originalTimeEnds[symbol]
					timeEnds[symbol] = timeStarts[symbol].Add(bn.baseTimeframe.Duration * multiplier)
				} else {
					firstLoop = false
					// Keep timeStart as original value
					// need to multiply by 394200 if the start time is zero because Binance's API doesn't allow for any difference that is lower. Big yikes
					timeEnds[symbol] = timeStarts[symbol].Add(bn.baseTimeframe.Duration * multiplier)
					multiplier = 300
				}
				// Makes sense for 1 minute time frame but not for larger ones...
				// TODO: Makes it so that it works for all timeframes
				// if timeStart.After(time.Now().UTC()) {
				// 	timeStart = originalTimeEnd
				// 	timeEnd = time.Now().UTC()
				// 	// Check multiplier
				// 	// If multiplier is bigger than 1, make it smaller until need be
				// 	if multiplier == 1 {
				// 		slowDown = true
				// 	} else {
				// 		multiplier /= 2
				// 	}
				// }

				// Do the same as above
				if timeEnds[symbol].After(time.Now().UTC()) {
					timeEnds[symbol] = time.Now().UTC()
					// Check multiplier
					// If multiplier is bigger than 1, make it one now so that it increments only by 1 for the next one and then move on.
					// Change multiplier to 1 since in theory we are getting to the most recent candle right now
					multiplier = 1
					slowDown = true

					// if multiplier != 1 {
					// } else {
					// 	// Slow down after we go through once with a multiplier of 1
					// }
				}
			} else {
				// Set to the :00 of previous TimeEnd to ensure that the complete candle that was not formed before is written
				originalTimeEnds[symbol] = originalTimeEndZeros[symbol]
			}

			// Sleep for the timeframe
			// Otherwise continue to call every second to backfill the data
			// Slow Down for 1 Duration period
			// Make sure last candle is formed
			if slowDown {
				timeEnds[symbol] = time.Now().UTC()
				timeStarts[symbol] = originalTimeEnds[symbol]

				year := timeEnds[symbol].Year()
				month := timeEnds[symbol].Month()
				day := timeEnds[symbol].Day()
				hour := timeEnds[symbol].Hour()
				minute := timeEnds[symbol].Minute()

				// To prevent gaps (ex: querying between 1:31 PM and 2:32 PM (hourly)would not be ideal)
				// But we still want to wait 1 candle afterwards (ex: 1:01 PM (hourly))
				// If it is like 1:59 PM, the first wait sleep time will be 1:59, but afterwards would be 1 hour.
				// Main goal is to ensure it runs every 1 <time duration> at :00
				switch originalInterval {
				case "1Min":
					timeEnds[symbol] = time.Date(year, month, day, hour, minute, 0, 0, time.UTC)
				case "1H":
					timeEnds[symbol] = time.Date(year, month, day, hour, 0, 0, 0, time.UTC)
				case "1D":
					timeEnds[symbol] = time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
				default:
					fmt.Printf("Incorrect format: %v\n", originalInterval)
				}
				waitTills[symbol] = timeEnds[symbol].Add(bn.baseTimeframe.Duration)

				timeStartM := timeStarts[symbol].UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
				timeEndM := timeEnds[symbol].UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))

				// Make sure you get the last candle within the timeframe.
				// If the next candle is in the API call, that means the previous candle has been fully formed
				// (ex: if we see :00 is formed that means the :59 candle is fully formed)
				gotCandle := false
				for !gotCandle {
					rates, err := client.NewKlinesService().Symbol(symbols[0] + baseCurrency).Interval(timeInterval).StartTime(timeStartM).Do(context.Background())
					if err != nil {
						fmt.Printf("Response error: %v\n", err)
						time.Sleep(time.Minute)
					}

					if len(rates) > 0 && rates[len(rates)-1].OpenTime-timeEndM >= 0 {
						gotCandle = true
					}
				}

				originalTimeEndZeros[symbol] = timeEnds[symbol]
				// Change timeEnd to the correct time where the last candle is formed
				timeEnds[symbol] = time.Now().UTC()
			}

			// Repeat since slowDown loop won't run if it hasn't been past the current time
			timeStartM = timeStarts[symbol].UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
			timeEndM = timeEnds[symbol].UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))

			fmt.Printf("Requesting %s %v - %v\n", symbol, timeStarts[symbol], timeEnds[symbol])

			// Grab price data
			rates, err := client.NewKlinesService().Symbol(symbol + baseCurrency).Interval(timeInterval).StartTime(timeStartM).EndTime(timeEndM).Do(context.Background())
			// fmt.Printf("rates %v\n", rates)

			// For some reason Binance, doesn't allow for small time differences between starttimes and endtimes in some cases (seemingly in the beginning)
			// If that is the case, get as many candles as possible.
			// if rates == nil {
			// 	fmt.Printf("Empty s: %v v: %v\n", timeStartM, timeEndM)
			// 	rates, err = client.NewKlinesService().Symbol(symbol + baseCurrency).Interval(timeInterval).StartTime(timeStartM).Do(context.Background())
			// }

			if err != nil {
				fmt.Printf("Response error: %v\n", err)
				// time.Sleep(time.Minute)
				// Go back to last time
				timeStarts[symbol] = originalTimeStarts[symbol]
				// Check error message if it contains
				if strings.Contains(err.Error(), "network is unreachable") {
					updating = true
				}
				break
			}
			// if len(rates) == 0 {
			// 	fmt.Printf("len(rates) == 0\n")
			// 	continue
			// }
			if !updating {
				openTime := make([]int64, 0)
				open := make([]float64, 0)
				high := make([]float64, 0)
				low := make([]float64, 0)
				close := make([]float64, 0)
				volume := make([]float64, 0)

				for _, rate := range rates {
					errorsConversion = errorsConversion[:0]
					// fmt.Printf("rate %v\n", rate)
					// if nil, do not append to list
					if rate.OpenTime != 0 && rate.Open != "" &&
						rate.High != "" && rate.Low != "" &&
						rate.Close != "" && rate.Volume != "" {
						openTime = append(openTime, convertMillToTime(rate.OpenTime).Unix())
						open = append(open, convertStringToFloat(rate.Open))
						high = append(high, convertStringToFloat(rate.High))
						low = append(low, convertStringToFloat(rate.Low))
						close = append(close, convertStringToFloat(rate.Close))
						volume = append(volume, convertStringToFloat(rate.Volume))

						for _, e := range errorsConversion {
							if e != nil {
								return
							}
						}
					} else {
						fmt.Printf("No value in rate %v\n", rate)
					}
				}

				validWriting := true
				if len(openTime) == 0 || len(open) == 0 || len(high) == 0 || len(low) == 0 || len(close) == 0 || len(volume) == 0 {
					validWriting = false
				}
				// if data is nil, do not write to csm
				if validWriting {
					cs := io.NewColumnSeries()
					// Remove last incomplete candle if it exists since that is incomplete
					// Since all are the same length we can just check one
					// We know that the last one on the list is the incomplete candle because in
					// the gotCandle loop we only move on when the incomplete candle appears which is the last entry from the API
					if slowDown && len(openTime) > 1 {
						openTime = openTime[:len(openTime)-1]
						open = open[:len(open)-1]
						high = high[:len(high)-1]
						low = low[:len(low)-1]
						close = close[:len(close)-1]
						volume = volume[:len(volume)-1]
					}

					cs.AddColumn("Epoch", openTime)
					cs.AddColumn("Open", open)
					cs.AddColumn("High", high)
					cs.AddColumn("Low", low)
					cs.AddColumn("Close", close)
					cs.AddColumn("Volume", volume)
					csm := io.NewColumnSeriesMap()
					tbk := io.NewTimeBucketKey(symbol + "/" + bn.baseTimeframe.String + "/OHLCV")
					csm.AddColumnSeries(*tbk, cs)
					executor.WriteCSM(csm, false)
				}
			}

			// IF Binance is updating their API, sleep for an hour and wait till it's complete...
			// Keep checking every hour until update is finished
			if updating {
				finishedUpdating := false
				for !finishedUpdating {
					_, err := client.NewKlinesService().Symbol(symbol).Interval(timeInterval).StartTime(timeStartM).Do(context.Background())
					if err != nil {
						fmt.Printf("Response error: %v\n", err)
						time.Sleep(time.Hour)
					} else {
						finishedUpdating = true
					}
				}
			} else {
				if slowDown {
					// Sleep till next :00 time
					time.Sleep(waitTills[symbol].Sub(time.Now().UTC()))
				} else {
					// Binance rate limit is 20 reequests per second so this shouldn't be an issue.
					time.Sleep(time.Second)
				}
			}

		}

	}
}

func main() {
	// symbol := "XRP"
	// interval := "1m"
	// baseCurrency := "USDT"
	// var start int64 = 1483228800000
	// var end int64   = 1483264800000
	//
	// client := binance.NewClient("", "")
	// klines, err := client.NewKlinesService().Symbol(symbol + baseCurrency).Interval(interval).StartTime(start).EndTime(end).Do(context.Background())
	// //
	// // klines, err := client.NewKlinesService().Symbol(symbol + baseCurrency).
	// // 	Interval(interval).Do(context.Background())
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// for _, k := range klines {
	// 	fmt.Println(k)
	// }
	// symbols := getAllSymbols("USDT")
	// for _, s := range symbols {
	// 	fmt.Println(s)
	// }
}
