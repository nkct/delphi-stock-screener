import pandas as pd
import sqlite3 as db
import argparse
import json
import sys
import logging as log

from src import utils
from src import helpers
from src.fetch import fetch
from src.sort import sort
from src.filter import filter as delphi_filter

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
        helpers.clear(conn, args.clear)
        sys.exit("Cleared, exiting")

    if args.properties:
        print(helpers.available_properties())
        sys.exit("Properties displayed, exiting")

    if args.indices:
        print(helpers.indices(args.indices, indices))
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
        helpers.new_index(args.new_index, indices, symbols)
        sys.exit("New index created, exiting")

    if args.delete_index:
        helpers.delete_index(args.delete_index, indices)
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