import configparser
import psycopg2
import pandas as pd
from sql_queries import analytic_queries, analytic_questions


def analyse(cur, conn):
    for question, query in zip(analytic_questions, analytic_queries):
        print(question)
        cur.execute(query)
        results = cur.fetchall()  # Fetch all rows of the executed query
        columns = [desc[0] for desc in cur.description]  # Get column names from the cursor description
        df = pd.DataFrame(results, columns=columns)
        print(df)
        print('\n')
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
    
    analyse(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()