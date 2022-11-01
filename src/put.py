import pandas as pd
import sqlite3 as db

from src import utils

def put(df: pd.DataFrame, database = utils.get_database(), table = utils.get_table()):
    conn = db.connect(database)
    cur = conn.cursor()

    #df = df.reset_index()
    df.dropna(axis = 1, inplace = True)

    columns = utils.tuple_to_sql_tuple_string(tuple(df.columns.values.tolist()))
    sql_columns = columns.replace("'", "")

    values = utils.tuple_to_sql_tuple_string(tuple(df.iloc[0].tolist()))

    while True:
        try:
            changes = []
            for column in df.columns.values.tolist():
                changes.append(f"{column} = '{df.loc[0, column]}'")
            changes = str(changes)[1:-1].replace("\"", "")

            cur.execute(f"""
                            UPDATE {table}
                            SET {changes}
                            WHERE symbol = '{df.loc[0, "symbol"]}'
                        """)
            
            conn.commit()

            if cur.rowcount == 0:
                cur.execute(f"""
                                INSERT INTO {table}
                                {sql_columns}
                                VALUES
                                {values};
                            """)

                conn.commit()

            print(f"Inserting into table: {table}, Rows affected: {cur.rowcount}")

            cur.close()
            conn.close()

            break
        except db.OperationalError as e:
            #column not found
            if str(e).startswith("no such column:"):
                column = str(e)[16:]
                cur.execute(f"""
                                ALTER TABLE {table}
                                ADD {column} TEXT
                            """)
                continue
            # table not found
            elif str(e).startswith("no such table:"):
                cur.execute(f"""
                                CREATE TABLE {table} (
                                    symbol TEXT
                                )
                            """)
                continue
            else:
                print(f"Error while inserting into {table}: {e}")
                break