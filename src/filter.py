import logging as log

from src import utils
from src.fetch import fetch

def filter(symbols, filters):
    log.debug(f"filter({symbols}, {filters})")

    # taking a slice to avoid mutating during iteration
    for symbol in symbols[:]:
        for filter in filters:
            filter = filter.split()

            lval = utils.alias(filter[0])
            rval = utils.alias(filter[2])

            lval_fetch = fetch([symbol], [lval])[lval]
            rval_fetch = fetch([symbol], [rval])[rval]

            # if fetch was succesful, extracts value
            lval = lval_fetch[0] if not lval_fetch.empty else lval
            rval = rval_fetch[0] if not rval_fetch.empty else rval

            # type normalization
            lval = str(lval)
            rval = str(rval)
                
            lval = float(lval) if lval.isnumeric() else lval
            rval = float(rval) if rval.isnumeric() else rval

            op = filter[1]
            
            if not utils.eval(lval, op, rval):
                # if is nessecary because multiple passes over symbols are made
                if symbol in symbols:
                    symbols.remove(symbol)

    return symbols