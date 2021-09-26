import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for table, query in copy_table_queries.items():
        print(f'Loading {table}')
        cur.execute(query)
        conn.commit()   


def insert_tables(cur, conn):
    for table, query in insert_table_queries.items():
        print(f'Inserting {table}')
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Loading Tables!\n')
    # load_staging_tables(cur, conn)
    print('\nInserting Tables!\n')
    insert_tables(cur, conn)
    print('Done & Closing')
    conn.close()


if __name__ == "__main__":
    main()