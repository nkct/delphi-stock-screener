import logging as log

from src import utils
from src.fetch import fetch

def filter(symbols, filters):
    log.info(f"filter({symbols}, {filters})")

    for symbol in symbols[:]:
            for filter in filters:
                filter = filter.split()

                lval = utils.alias(filter[0])
                rval = utils.alias(filter[2])

                lval = float(fetch([symbol], [lval])[lval][0])
                op = filter[1]
                rval = float(filter[2] if filter[2].isnumeric() else fetch([symbol], [rval])[rval][0])


                if not utils.eval(lval, op, rval):
                    # is this if even nessecary?
                    if symbol in symbols:
                        symbols.remove(symbol)

    return symbols