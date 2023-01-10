import pandas as pd
import sqlite3 as db
import logging as log

from src import utils

def put(df: pd.DataFrame, database = utils.get_database(), table = utils.get_table()):
    log.debug(f"put(\n{df},\n {database}, {table})")
    
    conn = db.connect(database)
    cur = conn.cursor()

    df = df.dropna(how="all").fillna("Null")


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


    # select the values that are not in the database
    sql_columns = utils.tuple_to_sql_tuple_string(tuple(columns)).replace("'", "")[1:-1]
    symbols = utils.tuple_to_sql_tuple_string(tuple(df['symbol'].tolist()))

    cur.execute(f"SELECT {sql_columns} FROM {table} WHERE symbol IN {symbols}")
    missing_values = []
    fetch_result = cur.fetchall()
    for row_index, row in enumerate(df.iloc):
        if row_index >= len(fetch_result):
            missing_values.append(list(row.values))
        else:
            missing_values.append([
                value for value in row if
                list(row.values).index(value) >= len(fetch_result[row_index])
            ])

    # if there are any missing values, insert them
    if any(missing_values):
        missing_values = [utils.tuple_to_sql_tuple_string(tuple(row)) + "," for row in missing_values]
        # remove the trailing comma
        missing_values[-1] = missing_values[-1][:-1]
        insertable_values = "".join(f"{row} \n" for row in missing_values)

        cur.execute(f"""
                        INSERT INTO {table}
                        ({sql_columns})
                        VALUES
                        {insertable_values};
                    """)
        conn.commit()
        log.info(f"New Inserted into table: {table}, Rows affected: {cur.rowcount}")


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