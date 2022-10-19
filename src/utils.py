import ast
import operator
import json
import pandas as pd
import logging as log
import sqlite3 as db

# converts to string and removes the trailing comma in case of a one element tuple
def tuple_to_sql_tuple_string(tuple: tuple):
    log.debug(f"tuple_to_sql_tuple_string({tuple})")

    out = str(tuple)
    

    if len(tuple) == 1:
        out = out[:-2] + ")"

    return out

# adds trailing comma unless empty then converts to tuple
def sql_tuple_string_to_tuple(str: str):
    log.debug(f"tuple_to_sql_tuple_string({str})")

    out = str

    # adds trailing comma unless the tuple is empty
    if str[-2] == "'":
        out = out[:-1] + ",)"

    out = ast.literal_eval(out)    

    return out

# takes two values and executes the provided comperative operation on them
def eval(lval, op, rval):
    log.debug(f"eval({lval}, {op}, {rval})")

    ops = {
        ">": operator.gt,
        "<": operator.lt,
        ">=": operator.ge,
        "<=": operator.le,
        "==": operator.eq,
        "!=": operator.ne,
    }
    return ops[op](lval, rval)

# load the aliases.json file to a dict
def load_aliases():
    aliases = {
        # as json does not allow for tuple keys they are stored as a string and then parsed
        ast.literal_eval(k): v 
        for k, v in json.loads (
            open("aliases.json")
            .read()
        ).items()
    }

    return aliases

# checks if the provided word has any synonyms, if so, returns the base meaning, if not, returns input
def alias(alias: str):
    aliases = load_aliases()

    for key in aliases.keys():
        if alias.lower() in key and alias.lower() != aliases[key]:
            log.info(f"Aliasing {alias} to {aliases[key]}")
            return aliases[key]

    
    return alias

# drop the provided table in the provided databse
def clear(conn: db.Connection, table: str):
    cur = conn.cursor()

    cur.execute(f"""
                    DROP TABLE {table}
                """)

    log.info(f"Clearing table: {table}, Rows affected: {cur.rowcount}")

def scrape_sp500():
    sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0].loc[:, "Symbol"].tolist()