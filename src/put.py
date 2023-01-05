import pandas as pd
import sqlite3 as db
import logging as log

from src import utils

def put(df: pd.DataFrame, database = utils.get_database(), table = utils.get_table()):
    log.debug(f"put(\n{df},\n {database}, {table})")
    
    conn = db.connect(database)
    cur = conn.cursor()

    df = df.fillna("Null")


    # if table doesnt exist, create it
    cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
    if not cur.fetchall():
        cur.execute(f"""
                        CREATE TABLE {table} (
                            symbol TEXT
                        )
                    """)
        log.info(f"New Created table {table}")


    # add columns that arent in the table
    cur.execute(f"PRAGMA table_info({table})")
    columns_currently_in_table = [col[1] for col in cur.fetchall()]

    columns = df.columns.values.tolist()

    missing_columns = [col for col in columns if col not in columns_currently_in_table]

    if missing_columns:
        column_query_text = ""
        for column in missing_columns:
            column_query_text += f"ALTER TABLE {table} ADD {column} TEXT; \n"

        cur.executescript(f"{column_query_text}")
        log.info(f"New Altered table to add columns: {missing_columns}")


    while True:
        try:
            for row in df.iloc:
                changes = []
                for value in row:
                    # get trid of unnecessary floats
                    if isinstance(value, float) and value.is_integer():
                        value = int(value)
                    changes.append(f"{row.index[list(row.values).index(value)]} = '{value}'")
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
                    values += utils.tuple_to_sql_tuple_string(tuple(
                        map(
                            lambda value: int(value) if isinstance(value, float) and value.is_integer() else value, 
                            row.tolist()))
                        ) + ","
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