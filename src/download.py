import sys
import pandas as pd
import yfinance as yf
import logging as log

def download(symbol, properties):
    log.debug(f"download({symbol}, {properties})")

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

    default_properties = ["symbol", "shortName", "currentPrice", "sector", "marketCap"]
    if "*" in properties:
        properties = default_properties

    ticker = yf.Ticker(symbol)
    section = None
    out = {}

    for property in properties:
        for key in property_table:
            if property == "symbol":
                out["symbol"] = symbol
                continue

            if property in key:
                try:
                    section = getattr(ticker, property_table[key])
                except ConnectionError:
                    sys.exit("Connection error")

                out[property] = section.get(property)
                break
            else:
                sys.exit(f"Property not available: {property}")

    df = pd.DataFrame(out, columns = properties, index = [0])

    return df

    """
    print(yf.Ticker("AAPL").actions)
    print(yf.Ticker("AAPL").analysis)
    print(yf.Ticker("AAPL").balancesheet)
    print(yf.Ticker("AAPL").calendar)
    print(yf.Ticker("AAPL").cashflow)
    print(yf.Ticker("AAPL").dividends)
    print(yf.Ticker("AAPL").earnings)
    print(yf.Ticker("AAPL").financials)
    print(yf.Ticker("AAPL").institutional_holders)
    print(yf.Ticker("AAPL").isin)
    print(yf.Ticker("AAPL").major_holders)
    print(json.dumps(yf.Ticker("AAPL").news, indent = 4))
    print(yf.Ticker("AAPL").mutualfund_holders)
    print(yf.Ticker("AAPL").options)
    print(yf.Ticker("AAPL").quarterly_earnings)
    print(yf.Ticker("AAPL").quarterly_financials)
    print(yf.Ticker("AAPL").quarterly_balance_sheet)
    print(yf.Ticker("AAPL").quarterly_cashflow)
    print(yf.Ticker("AAPL").recommendations)
    print(yf.Ticker("AAPL").shares)
    print(yf.Ticker("AAPL").sustainability)
    print(yf.Ticker("AAPL").splits)
    """