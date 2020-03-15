import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    execute COPY command to read JSON in S3 to staging area
    Args:
        cur (psycopg2 connection cursor): cursor for the database connection
        conn (psycopg2 connection): connection to database
    Returns:
        `None`: actions performed, but no return value
    """
    for query in copy_table_queries:
        print (f"Processing load staging: {query}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ 
    Extracts data from staging tables and inserts it into star schema.
    
    execute insert statement to populate table from staging area
    
    Args:
        cur (psycopg2 connection cursor): cursor for the database connection
        conn (psycopg2 connection): connection to database
    Returns:
        `None`: actions performed, but no return value
    """
    for query in insert_table_queries:
        print (f"processing insert table: {query}")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
