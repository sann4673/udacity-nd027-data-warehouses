import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, counting_queries, tables, staging_tables, core_tables


def load_staging_tables(cur, conn):
    for staging_table, query in zip(staging_tables, copy_table_queries):
        print(f"Copying {staging_table}")
        try:
            cur.execute(query)
        except Exception as e:
            print(e)
        conn.commit()


def insert_tables(cur, conn):
    for core_table, query in zip(core_tables, insert_table_queries):
        print(f"Inserting {core_table}")
        try:
            cur.execute(query)
        except Exception as e:
            print(e)
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    count_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()