import logging as log

from src import utils
from src.fetch import fetch

def filter(symbols, filters):
    log.info(f"filter({symbols}, {filters})")

    for symbol in symbols[:]:
            for filter in filters:
                filter = filter.split()

                lval = float(fetch([symbol], [utils.alias(filter[0])])[utils.alias(filter[0])][0])
                op = filter[1]
                rval = int(filter[2])


                if not utils.eval(lval, op, rval):
                    # is this if even nessecary?
                    if symbol in symbols:
                        symbols.remove(symbol)

    return symbols