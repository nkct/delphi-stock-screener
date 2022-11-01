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

parser = argparse.ArgumentParser(formatter_class = argparse.RawTextHelpFormatter, add_help = False)
parser.add_argument("symbols", nargs = "?" if (
                                            "--clear" in sys.argv or 
                                            "-c" in sys.argv or 
                                            "--properties" in sys.argv or 
                                            "-p" in sys.argv or 
                                            "--delete_index" in sys.argv or 
                                            "-d" in sys.argv or 
                                            "--indices" in sys.argv or
                                            "-i" in sys.argv
                                        ) else 1)
parser.add_argument("property", nargs = "*", default = "*")

parser.add_argument("-h", "--help", action = "help", default = argparse.SUPPRESS, help = "show this help message and exit\n\n")

parser.add_argument("-f", "--filter", nargs = "+", metavar = "FILTERS", help = "filter symbols to only those that fit the specified criteria \nusage: --filter \"[property] [operator] [value] \nexample --filter \"currentPrice < 200\"\"\n\n")

parser.add_argument("-s", "--sort", nargs = "+", metavar = "SORTERS", help = "sort symbols by the provided properties \nusage: --sort [property] \nexample: --sort currentPrice")
parser.add_argument("-D", "--descending", action = "store_true", help = "order sort results from largest to smallest\n\n")

parser.add_argument("-c", "--clear", metavar = "TABLE", help = "delete (clear) the provided table in the database and exit\n\n")

parser.add_argument("-p", "--properties", action = "store_true", help = "print all available properties and exit\n\n")

parser.add_argument("-n", "--new_index", nargs = 1, metavar = "INDEX_NAME", help = "make a new index containing the provided symbols and index name and exit")
parser.add_argument("-d", "--delete_index", nargs = 1, metavar = "INDEX_NAME", help = "delete the designated index and exit")
parser.add_argument("-i", "--indices", nargs = "?", default = False, const = "all", help = "display the designated indices and exit, defaults to all\n\n")

parser.add_argument("-v", "--version", action = "version", version = "Delphi 0.1.0")
args = parser.parse_args()

indices = json.load(open("indices.json"))

logging_level = log.WARNING
log.basicConfig(level = logging_level)


def run(indices, args):
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


    symbols = args.symbols[0].split()
    symbols = utils.alias_symbols(symbols)
    symbols = utils.indices_to_keys(symbols, indices)

    if args.filter:
        symbols = delphi_filter(symbols, args.filter)

    if args.sort:
        symbols = sort(symbols, args.sort, args.descending)

    if args.new_index:
        helpers.new_index(args.new_index, indices, symbols)
        sys.exit("New index created, exiting")

    if args.delete_index:
        helpers.delete_index(args.delete_index, indices)
        sys.exit("Index deleted, exiting")

    properties = utils.alias_properties(args.properties if args.properties else ["*"])

    out = fetch(symbols, properties)
    
    if out.empty:
        print("Nothing fits the specified criteria.")
    else:
        print(out)

    
run(indices, args)