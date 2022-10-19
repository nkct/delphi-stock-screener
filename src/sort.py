import logging as log

from src import utils
from src.fetch import fetch

def sort(symbols, sorters, descending):
    log.info(f"sort({symbols}, {sorters}, {descending})")

    sorters = list(map(utils.alias, sorters))

    if "symbol" not in sorters:
        sorters.append("symbol")

    log.debug(f"sorters after aliasing: {sorters}")

    df = fetch(symbols, sorters)
    df = df.sort_values(by = sorters, ascending = descending)

    symbols = df["symbol"].tolist()

    return symbols