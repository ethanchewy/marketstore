package main

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"time"

	binance "github.com/adshao/go-binance"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/golang/glog"
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
	QueryEnd      string   `json:"query_end"`
	BaseTimeframe string   `json:"base_timeframe"`
}

// BinanceFetcher is the main worker for Binance
type BinanceFetcher struct {
	config        map[string]interface{}
	symbols       []string
	baseCurrency  string
	queryStart    time.Time
	queryEnd      time.Time
	baseTimeframe *utils.Timeframe
}

// recast changes parsed JSON-encoded data represented as an interface to FetcherConfig structure
func recast(config map[string]interface{}) *FetcherConfig {
	data, _ := json.Marshal(config)
	ret := FetcherConfig{}
	json.Unmarshal(data, &ret)
	return &ret
}

//Convert string to float64 using strconv
func convertStringToFloat(str string) float64 {
	convertedString, err := strconv.ParseFloat(str, 64)
	//Store error in string array which will be checked in main fucntion later to see if there is a need to exit
	if err != nil {
		glog.Errorf("String to float error: %v", err)
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
		glog.Infof("Binance /exchangeInfo API error: %v", err)
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
	csm, _, err := reader.Read()
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
	var queryEnd time.Time
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

	if config.QueryEnd != "" {
		queryEnd = queryTime(config.QueryEnd)
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
		queryEnd:      queryEnd,
		baseTimeframe: utils.NewTimeframe(timeframeStr),
	}, nil
}

// Run grabs data in intervals from starting time to ending time.
// If query_end is not set, it will run forever.
func (bn *BinanceFetcher) Run() {
	symbols := bn.symbols
	client := binance.NewClient("", "")
	timeStart := time.Time{}
	finalTime := bn.queryEnd
	baseCurrency := bn.baseCurrency
	loopForever := false
	slowDown := false

	originalInterval := bn.baseTimeframe.String
	re := regexp.MustCompile("[0-9]+")
	re2 := regexp.MustCompile("[a-zA-Z]+")

	timeIntervalLettersOnly := re.ReplaceAllString(originalInterval, "")
	timeIntervalNumsOnly := re2.ReplaceAllString(originalInterval, "")

	correctIntervalSymbol := suffixBinanceDefs[timeIntervalLettersOnly]

	//If Interval is formmatted incorrectly
	if len(correctIntervalSymbol) <= 0 {
		glog.Errorf("Interval Symbol Format Incorrect. Setting to time interval to default '1Min'")
		correctIntervalSymbol = "1Min"
	}

	//Time end check
	if finalTime.IsZero() {
		finalTime = time.Now().UTC()
		loopForever = true
	}

	//Replace interval string with correct one with API call
	timeInterval := timeIntervalNumsOnly + correctIntervalSymbol

	for _, symbol := range symbols {
		tbk := io.NewTimeBucketKey(symbol + "/" + bn.baseTimeframe.String + "/OHLCV")
		lastTimestamp := findLastTimestamp(symbol, tbk)
		glog.Infof("lastTimestamp for %s = %v", symbol, lastTimestamp)
		if timeStart.IsZero() || (!lastTimestamp.IsZero() && lastTimestamp.Before(timeStart)) {
			timeStart = lastTimestamp
		}
	}

	for {
		if timeStart.IsZero() {
			if !bn.queryStart.IsZero() {
				timeStart = bn.queryStart
			} else {
				timeStart = time.Now().UTC().Add(-time.Hour)
			}
		} else {
			timeStart = timeStart.Add(bn.baseTimeframe.Duration * 300)
		}

		timeEnd := timeStart.Add(bn.baseTimeframe.Duration * 300)

		diffTimes := finalTime.Sub(timeEnd)

		// Reset time. Make sure you get all data possible
		// Will continue forever
		if diffTimes < 0 {
			timeStart = timeStart.Add(-bn.baseTimeframe.Duration * 300)
			if loopForever {
				finalTime = time.Now().UTC()
				timeEnd = finalTime
			} else {
				timeEnd = finalTime
			}
			slowDown = true
		}

		if diffTimes == 0 {
			glog.Infof("Got all data from: %v to %v", bn.queryStart, bn.queryEnd)
			glog.Infof("Continuing...")
		}

		var timeStartM int64
		var timeEndM int64

		timeStartM = timeStart.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
		timeEndM = timeEnd.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))

		for _, symbol := range symbols {
			glog.Infof("Requesting %s %v - %v", symbol, timeStart, timeEnd)

			rates, err := client.NewKlinesService().Symbol(symbol + baseCurrency).Interval(timeInterval).StartTime(timeStartM).EndTime(timeEndM).Do(context.Background())

			if err != nil {
				glog.Errorf("Response error: %v", err)
				glog.Infof("Problematic symbol %s", symbol)
				time.Sleep(time.Minute)
				// Go back to last time
				timeStart = timeStart.Add(-bn.baseTimeframe.Duration * 300)
				continue
			}
			if len(rates) == 0 {
				glog.Info("len(rates) == 0")
				continue
			}

			openTime := make([]int64, 0)
			open := make([]float64, 0)
			high := make([]float64, 0)
			low := make([]float64, 0)
			close := make([]float64, 0)
			volume := make([]float64, 0)

			for _, rate := range rates {
				errorsConversion = errorsConversion[:0]
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
			}

			cs := io.NewColumnSeries()
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

		// Sleep for half the timeframe
		// Otherwise continue to call every second
		if slowDown {
			time.Sleep(bn.baseTimeframe.Duration / 2)
		} else {
			time.Sleep(time.Second)
		}

	}
}

func main() {
	// symbol := "BTC"
	// interval := "1m"
	// baseCurrency := "USDT"
	//
	// client := binance.NewClient("", "")
	// klines, err := client.NewKlinesService().Symbol(symbol + baseCurrency).
	// 	Interval(interval).Do(context.Background())
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
