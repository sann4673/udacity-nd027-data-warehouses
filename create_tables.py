import configparser
import psycopg2
import pandas as pd
from sql_queries import create_table_queries, drop_table_queries, counting_queries, tables


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def count_tables(cur, conn):
    for table, query in zip(tables, counting_queries):
        cur.execute(query)
        count = cur.fetchone()[0]
        print(f"Counting {table}: {count}")
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    HOST = config.get('DB', 'HOST')
    DB_NAME = config.get('DB', 'DB_NAME')
    DB_USER = config.get('DB', 'DB_USER')
    DB_PASSWORD = config.get('DB', 'DB_PASSWORD')
    DB_PORT = config.get('DB', 'DB_PORT')

    conn = psycopg2.connect(f"host={HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    count_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()