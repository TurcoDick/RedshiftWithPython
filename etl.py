import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, query_tables

def load_staging_tables(cur, conn):
    """
    Data transfer from s3 to the redshift database for 2 stagings tables.    
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Transfer the data from the stagings tables to the other tables.
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        
def query_data(cur, conn):
    """
    Make a simple query of data in each table.
    """
    for query in query_tables:
        print("\n\n+++++++++++++++++++++++++++++++++++++++++\n")
        print(query)
        cur.execute(query)
        data = cur.fetchall()
        
        for item in data:
            print(item)
        
        
        
def main():
    """
    This method calls the methods responsible for data transfer, insertion and queries.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
   
    load_staging_tables(cur, conn)
    
    insert_tables(cur, conn)
    
    query_data(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()