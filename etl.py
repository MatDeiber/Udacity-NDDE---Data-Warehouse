import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    ''' 
    Function to load the staging tables - staging_events and
    staging_songs
    '''
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    ''' 
    Function to populate the fact and dimension tables
    '''
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Loading Staging Tables')
    load_staging_tables(cur, conn)
    print('Staging Tables Loaded Successfully')

    print('Inserting Data in Tables')
    insert_tables(cur, conn)
    print('Data Inserted Successfully')

    conn.close()


if __name__ == "__main__":
    main()