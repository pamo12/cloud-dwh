import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ executes drop table statements given in sql_queries.py
    param cur: the cursor of the database connection
    param conn: the connection to the database the tables should be dropped from
    return: None
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.DatabaseError as error:
            print(error)


def create_tables(cur, conn):
    """ executes create table statements given in sql_queries.py
    param cur: the cursor of the database connection
    param conn: the connection to the database the tables should be created in
    return: None
    """
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
