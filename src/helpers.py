# defined in this file are functions (and objects) that are:
# not large or significant enough to have their own file, interacting with the user

import json
import logging as log
import sqlite3 as db

import utils

# drop the provided table in the provided databse
def clear(conn: db.Connection, table: str):
    cur = conn.cursor()

    cur.execute(f"""
                    DROP TABLE {table}
                """)

    log.info(f"Clearing table: {table}, Rows affected: {cur.rowcount}")

def available_properties():
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

    return json.dumps(property_table, indent = 4)

def indices(chosen_indices, saved_indices):
    if chosen_indices == "all":
            return json.dumps(saved_indices, indent = 4)
    else:
        out = ""
        for key, value in saved_indices.items():
            if key in map(utils.alias, chosen_indices.split()):
                out += f"{key}: {value}\n"
        return out

def new_index(new_index, saved_indices, symbols):
    saved_indices[new_index[0]] = symbols
    with open("indices.json", "w") as f:
        json.dump(indices, f)

def delete_index(delete_index, saved_indices):
    saved_indices.pop(delete_index[0])
    with open("indices.json", "w") as f:
        json.dump(indices, f)
