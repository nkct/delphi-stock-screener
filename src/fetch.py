import pandas as pd
import sqlite3 as db

from src import utils

from src.download import download
from src.put import put

def fetch(symbol, property):
    raw_property = property

    conn = db.connect('database.db')
    cursor = conn.cursor()

    symbols = tuple(symbol)
    symbols = utils.tuple_to_sql_tuple_string(symbols)

    drop = False
    if "symbol" not in property and property[0] != "*":
        drop = True
        property.append("symbol")
    property = str(property).translate({ord(i): None for i in '\'[]'})

    try:
        for s in symbol:
            symbol_exists = cursor.execute(f"""
                                SELECT EXISTS(
                                    SELECT 1 
                                    FROM data 
                                    WHERE symbol='{s}')
                            """).fetchone()[0]
            if symbol_exists == 0:
                raise db.OperationalError

        cursor.execute(f"""
                            SELECT {property} FROM data
                            WHERE symbol IN {symbols}
                        """)

    except db.OperationalError:
        print("Properties not found, downloading")
        for s in symbol:
            put(download(s, raw_property))

        cursor.execute(f"""
                            SELECT {property} FROM data
                            WHERE symbol IN {symbols}
                        """)

    columns = [description[0] for description in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns = columns)

    df = df.set_index("symbol")
    df = df.reindex(symbol)
    df = df.reset_index(drop = drop)

    return df