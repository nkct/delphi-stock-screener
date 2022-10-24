import yfinance as yf
import pandas as pd
import math
import sqlite3 as db
import argparse
import json
import sys
import logging as log

from src import utils
from src.fetch import fetch
from src.sort import sort
from src.filter import filter as delphi_filter

def millify(n):
    if n is None:
        return "n/a"
    
    millnames = ['',' Thousand',' Million',' Billion',' Trillion']
    n = float(n)
    millidx = max(0,min(len(millnames)-1,
                        int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))

    return '{:.0f}{}'.format(n / 10**(3 * millidx), millnames[millidx])

property_table = {
        (
            'zip', 
            'sector', 
            'fullTimeEmployees', 
            'longBusinessSummary', 
            'city', 
            'phone', 
            'state', 
            'country', 
            'companyOfficers', 
            'website', 
            'maxAge', 
            'address1', 
            'industry', 
            'ebitdaMargins', 
            'profitMargins', 
            'grossMargins', 
            'operatingCashflow', 
            'revenueGrowth', 
            'operatingMargins', 
            'ebitda', 
            'targetLowPrice', 
            'recommendationKey', 
            'grossProfits', 
            'freeCashflow', 
            'targetMedianPrice', 
            'currentPrice', 
            'earningsGrowth', 
            'currentRatio', 
            'returnOnAssets', 
            'numberOfAnalystOpinions', 
            'targetMeanPrice', 
            'debtToEquity', 
            'returnOnEquity', 
            'targetHighPrice', 
            'totalCash', 
            'totalDebt', 
            'totalRevenue', 
            'totalCashPerShare', 
            'financialCurrency', 
            'revenuePerShare', 
            'quickRatio', 
            'recommendationMean', 
            'exchange', 
            'shortName', 
            'longName', 
            'exchangeTimezoneName', 
            'exchangeTimezoneShortName', 
            'isEsgPopulated', 
            'gmtOffSetMilliseconds', 
            'quoteType', 
            'symbol', 
            'messageBoardId', 
            'market', 
            'annualHoldingsTurnover', 
            'enterpriseToRevenue', 
            'beta3Year', 
            'enterpriseToEbitda', 
            '52WeekChange', 
            'morningStarRiskRating', 
            'forwardEps', 
            'revenueQuarterlyGrowth', 
            'sharesOutstanding', 
            'fundInceptionDate', 
            'annualReportExpenseRatio', 
            'totalAssets', 
            'bookValue', 
            'sharesShort', 
            'sharesPercentSharesOut', 
            'fundFamily', 
            'lastFiscalYearEnd', 
            'heldPercentInstitutions', 
            'netIncomeToCommon', 
            'trailingEps', 
            'lastDividendValue', 
            'SandP52WeekChange', 
            'priceToBook', 
            'heldPercentInsiders', 
            'nextFiscalYearEnd', 
            'yield', 
            'mostRecentQuarter', 
            'shortRatio', 
            'sharesShortPreviousMonthDate', 
            'floatShares', 
            'beta', 
            'enterpriseValue', 
            'priceHint', 
            'threeYearAverageReturn', 
            'lastSplitDate', 
            'lastSplitFactor', 
            'legalType', 
            'lastDividendDate', 
            'morningStarOverallRating', 
            'earningsQuarterlyGrowth', 
            'priceToSalesTrailing12Months', 
            'dateShortInterest', 
            'pegRatio', 
            'ytdReturn', 
            'forwardPE', 
            'lastCapGain', 
            'shortPercentOfFloat', 
            'sharesShortPriorMonth', 
            'impliedSharesOutstanding', 
            'category', 
            'fiveYearAverageReturn', 
            'previousClose', 
            'regularMarketOpen', 
            'twoHundredDayAverage', 
            'trailingAnnualDividendYield', 
            'payoutRatio', 
            'volume24Hr', 
            'regularMarketDayHigh', 
            'navPrice', 
            'averageDailyVolume10Day', 
            'regularMarketPreviousClose', 
            'fiftyDayAverage', 
            'trailingAnnualDividendRate', 
            'open', 
            'toCurrency', 
            'averageVolume10days', 
            'expireDate', 
            'algorithm', 
            'dividendRate', 
            'exDividendDate', 
            'circulatingSupply', 
            'startDate', 
            'regularMarketDayLow', 
            'currency', 
            'trailingPE', 
            'regularMarketVolume', 
            'lastMarket', 
            'maxSupply', 
            'openInterest', 
            'marketCap', 
            'volumeAllCurrencies', 
            'strikePrice', 
            'averageVolume', 
            'dayLow', 
            'ask', 
            'askSize', 
            'volume', 
            'fiftyTwoWeekHigh', 
            'fromCurrency', 
            'fiveYearAvgDividendYield', 
            'fiftyTwoWeekLow', 
            'bid', 
            'tradeable', 
            'dividendYield', 
            'bidSize', 
            'dayHigh', 
            'coinMarketCapLink', 
            'regularMarketPrice', 
            'preMarketPrice', 
            'logo_url', 
            'trailingPegRatio'
        ): "info",
    }


parser = argparse.ArgumentParser()
parser.add_argument("symbols")
parser.add_argument("property", nargs = "*", default = "*")

parser.add_argument("-f", "--filter", nargs = "+")

parser.add_argument("-s", "--sort", nargs = "+")
parser.add_argument("-D", "--descending", action = "store_true")

parser.add_argument("-c", "--clear")

parser.add_argument("-p", "--properties", action = "store_true")

parser.add_argument("-n", "--new_index", nargs = 1)

parser.add_argument("-d", "--delete_index", nargs = 1)

parser.add_argument("-i", "--indices", nargs = "?", default = False, const = "all")

parser.add_argument("-v", "--version", action = "version", version = "Delphi 0.1.0")
args = parser.parse_args()

indices = json.load(open("indices.json"))

logging_level = log.WARNING
log.basicConfig(level = logging_level)


def run(indices, args):
    out = pd.DataFrame()

    conn = db.connect("database.db")

    if args.clear:
        utils.clear(conn, args.clear)
        sys.exit("Cleared, exiting")

    if args.properties:
        print(json.dumps(property_table, indent = 4))
        sys.exit("Properties displayed, exiting")

    if args.indices:
        if args.indices == "all":
            print(json.dumps(indices, indent = 4))
        else:
            for key, value in indices.items():
                if key in map(utils.alias, args.indices.split()):
                    print(f"{key}: {value}")
        sys.exit("Indices displayed, exiting")


    symbols = args.symbols.split()

    for symbol in symbols:
        symbols[symbols.index(symbol)] = symbol.upper()

    new_symbols = []
    for symbol in symbols[:]:
        symbol = utils.alias(symbol)
        if symbol in indices.keys():
            new_symbols += indices[symbol]
        else:
            new_symbols += [symbol]
    symbols = new_symbols

    if args.filter:
        delphi_filter(symbols, args.filter)

    if args.sort:
        sort(symbols, args.sort, args.descending)

    if args.new_index:
        indices[args.new_index[0]] = symbols
        with open("indices.json", "w") as f:
            json.dump(indices, f)
        sys.exit("New index created, exiting")

    if args.delete_index:
        indices.pop(args.delete_index[0])
        with open("indices.json", "w") as f:
            json.dump(indices, f)
        sys.exit("Index deleted, exiting")

    properties = []
    for prop in args.property:
        properties += [utils.alias(prop)]
    out = fetch(symbols, properties)
    
    if out.empty:
        print("Nothing fits the specified criteria.")
    else:
        print(out)

    
run(indices, args)