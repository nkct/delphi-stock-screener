import pandas as pd
import sqlite3 as db
import logging as log

from src import utils

def put(df: pd.DataFrame, database = utils.get_database(), table = utils.get_table()):
    conn = db.connect(database)
    cur = conn.cursor()

    #df = df.reset_index()
    df.dropna(axis = 1, inplace = True)

    while True:
        try:
            for row in df.iloc:
                changes = []
                for value in row:
                    changes.append(f"{row[row == value].index[0]} = '{value}'")
                changes = str(changes)[1:-1].replace("\"", "")

                cur.execute(f"""
                                UPDATE {table}
                                SET {changes}
                                WHERE symbol = '{row[0]}'
                            """)
                
                conn.commit()


            if cur.rowcount == 0:

                columns = utils.tuple_to_sql_tuple_string(tuple(df.columns.values.tolist()))
                sql_columns = columns.replace("'", "")

                values = ""
                for row in df.head(-1).iloc:
                    values += utils.tuple_to_sql_tuple_string(tuple(row.tolist())) + ","
                values += utils.tuple_to_sql_tuple_string(tuple(df.iloc[-1].tolist()))

                cur.execute(f"""
                                INSERT INTO {table}
                                {sql_columns}
                                VALUES
                                {values};
                            """)

                conn.commit()

                log.info(f"Inserted into table: {table}, Rows affected: {cur.rowcount}")
            else:
                log.info(f"Updated values in table table: {table}, Rows affected: {cur.rowcount}")


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
                log.info(f"Altered table to add column: {column}")
                continue
            # table not found
            elif str(e).startswith("no such table:"):
                cur.execute(f"""
                                CREATE TABLE {table} (
                                    symbol TEXT
                                )
                            """)
                log.info(f"Created table {table}")
                continue
            else:
                log.error(f"Error while inserting into {table}: {e}")
                break