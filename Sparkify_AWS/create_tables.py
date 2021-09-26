import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """[This function drops all tables from Redshift db]

    Args:
        cur ([object]): [Cursor for database]
        conn ([object]): [Connection to database]
    """       
    for table, query in drop_table_queries.items():
        cur.execute(query)
        conn.commit()
        print(f'Dropping {table}')


def create_tables(cur, conn):
    """[This function creates all tables on defined Redshift db]

    Args:
        cur ([object]): [Cursor for database]
        conn ([object]): [Connection to database]
    """         
    for table, query in create_table_queries.items():
        cur.execute(query)
        conn.commit()
        print(f'Creating {table}')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Starting to Drop Tables!\n')
    drop_tables(cur, conn)
    print('\nStarting to Create Tables!\n')
    create_tables(cur, conn)
    print('\nDone: Closing session!')
    conn.close()


if __name__ == "__main__":
    main()