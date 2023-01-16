import logging as log

from src.fetch import fetch

def sort(symbols, sorters, descending):
    log.debug(f"sort({symbols}, {sorters}, {descending})")

    if "symbol" not in sorters:
        sorters.append("symbol")

    df = fetch(symbols, sorters)
    df = df.sort_values(by = sorters, ascending = descending)

    symbols = df["symbol"].tolist()

    return symbols