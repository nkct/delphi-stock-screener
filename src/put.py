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


    sql_columns = utils.tuple_to_sql_tuple_string(tuple(columns)).replace("'", "")[1:-1]
    symbols = utils.tuple_to_sql_tuple_string(tuple(df['symbol'].tolist()))

    # select the values that are not in the database and those that differ from the database
    cur.execute(f"SELECT {sql_columns} FROM {table} WHERE symbol IN {symbols}")
    missing_values = []
    differing_values = []
    fetch_result = cur.fetchall()
    for row_index, row in enumerate(df.iloc):
        values = utils.flaten_floats(list(row.values))
        if row_index >= len(fetch_result):
            missing_values.append(values)
        else:
            missing_values.append([])
            differing_values.append([])
            for value in row:
                if values.index(value) >= len(fetch_result[row_index]):
                    missing_values[row_index].append(value)

                if (not (values.index(value) >= len(fetch_result[row_index])) and
                    value not in fetch_result[row_index] or
                    row.index[values.index(value)] == "symbol"):
                        differing_values[row_index].append(value)


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

    if any(differing_values):
        for index, row in enumerate(differing_values):
            changes = []
            for value in row:
                # get trid of unnecessary floats
                if isinstance(value, float) and value.is_integer():
                    value = int(value)
                changes.append(f"{df.iloc[index].index[row.index(value)]} = '{value}'")
            changes = str(changes)[1:-1].replace("\"", "")

            cur.execute(f"""
                            UPDATE {table}
                            SET {changes}
                            WHERE symbol = '{row[0]}'
                        """)
        
            conn.commit()
            log.info(f"New Updated values in table table: {table}, Rows affected: {cur.rowcount}") 