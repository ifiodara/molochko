import logging
import psycopg2
from psycopg2 import sql
import psycopg2.extras

def get_known_manufactures(connection_var, supply_schema, manufactures_table, extract_category):
    try:
        conn = connection_var
        cursor = conn.cursor()
        cursor.execute(sql.SQL(""" select distinct name_part, manufacture from {0}.{1} where extract_category = {2}""")
            .format(sql.Identifier(supply_schema), sql.Identifier(manufactures_table),sql.Literal(extract_category)))
        rows = cursor.fetchall()
        logging.info("Got {0} manufactures".format(len(rows)))
        return rows
    except Exception as e:
        logging.error("Can't get manufactures list from DB")
        logging.error(e)
        return None

def connect_to_db(connection_string):
    try:
        conn = psycopg2.connect(connection_string)
        logging.info("Connected to DB")
        conn.autocommit = True
        return conn
    except Exception as e:
        logging.error("Can't connect to DB")
        logging.error(e)

def batch_insert_data(conn, result, schema, table, column_list):
    try:
        cursor = conn.cursor()
        columns = column_list.split(', ')
        result_length = len(result)

        target = sql.SQL('.').join([sql.Identifier(schema),sql.Identifier(table)])
        sql_columns = sql.SQL(', ').join(map(sql.Identifier, columns))

        psycopg2.extras.execute_values(cursor,sql.SQL(" insert into {0} ({1}) values %s").format(
            target, sql_columns).as_string(conn), result,page_size=result_length)

        rows_inserted = cursor.rowcount
        logging.info("Inserted {0} rows of data to {1}.{2}".format(rows_inserted,schema, table))
    except Exception as e:
        logging.error("Can't insert data to DB")
        logging.error(e)
    assert rows_inserted == len(result), logging.error("ASSERTION_ERROR: Got {0} rows but inserted {1} rows".format(result_length, rows_inserted))
