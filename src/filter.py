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

                lval_fetch = fetch([symbol], [lval])[lval]
                rval_fetch = fetch([symbol], [rval])[rval]

                lval = float(lval) if lval.isnumeric() else lval
                rval = float(rval) if rval.isnumeric() else rval

                lval = lval_fetch[0] if not lval_fetch.empty else lval
                rval = rval_fetch[0] if not rval_fetch.empty else rval
                print(fetch(["SYM"], ["0"]))

                op = filter[1]

                print(lval)
                print(type(rval))
                if not utils.eval(lval, op, rval):
                    # is this if even nessecary?
                    if symbol in symbols:
                        symbols.remove(symbol)

    return symbols