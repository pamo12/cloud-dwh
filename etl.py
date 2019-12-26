import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ executes the copy_table_queries statements given in sql_queries.py
    param cur: the cursor of the database connection
    param conn: the connection to the database the tables should be dropped from
    return: None
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.DatabaseError as error:
            print(error)


def insert_tables(cur, conn):
    """ executes the insert_table_queries given in sql_queries.py
    param cur: the cursor of the database connection
    param conn: the connection to the database the tables should be dropped from
    return: None
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.DatabaseError as error:
            print(error)


def main():
    config = configparser.ConfigParser()
    config.read('config/dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    except psycopg2.DatabaseError as error:
        print(error)
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
