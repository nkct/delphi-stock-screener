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
    table_check_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';"
    log.debug(f"table_check_query: {table_check_query}")
    cur.execute(table_check_query)
    if not cur.fetchall():
        create_table_query = f"""
                                  CREATE TABLE {table} (
                                      symbol TEXT
                                  )
                              """
        log.debug(f"create_table_query: {create_table_query}")
        cur.execute(create_table_query)
        log.info(f"Created table {table}")


    # add columns that arent in the table
    column_check_query = f"PRAGMA table_info({table})"
    log.debug(f"column_check_query: {column_check_query}")
    cur.execute(column_check_query)
    columns_currently_in_table = [col[1] for col in cur.fetchall()]

    columns = df.columns.values.tolist()

    missing_columns = [col for col in columns if col not in columns_currently_in_table]

    if missing_columns:
        add_columns_query = ""
        for column in missing_columns:
            data_type = "TEXT"
            if all([str(value).isnumeric() for value in df[column].values]):
                data_type = "INTEGER"
            add_columns_query += f"ALTER TABLE {table} ADD {column} {data_type}; \n"

        log.debug(f"add_columns_query: {add_columns_query}")
        cur.executescript(add_columns_query)
        log.info(f"Altered table to add columns: {missing_columns}")


    sql_columns = utils.tuple_to_sql_tuple_string(tuple(columns)).replace("'", "")[1:-1]
    symbols = utils.tuple_to_sql_tuple_string(tuple(df['symbol'].tolist()))

    # select the values that are not in the database and those that differ from the database
    values_check_query = f"SELECT {sql_columns} FROM {table} WHERE symbol IN {symbols}"
    log.debug(f"values_check_query: {values_check_query}")
    cur.execute(values_check_query)
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

        insert_values_query = f"""
                                   INSERT INTO {table}
                                   ({sql_columns})
                                   VALUES
                                   {insertable_values};
                               """
        log.debug(f"insert_values_query: {insert_values_query}")
        cur.execute(insert_values_query)
        conn.commit()
        log.info(f"Inserted into table: {table}, Rows affected: {cur.rowcount}")

    if any(differing_values):
        for index, row in enumerate(differing_values):
            changes = []
            for value in row:
                # get trid of unnecessary floats
                if isinstance(value, float) and value.is_integer():
                    value = int(value)
                changes.append(f"{df.iloc[index].index[row.index(value)]} = '{value}'")
            changes = str(changes)[1:-1].replace("\"", "")

            update_values_query = f"""
                                       UPDATE {table}
                                       SET {changes}
                                       WHERE symbol = '{row[0]}'
                                   """
            log.debug(f"update_values_query: {update_values_query}")
            cur.execute(update_values_query)
        
            conn.commit()
            log.info(f"Updated values in table table: {table}, Rows affected: {cur.rowcount}") 