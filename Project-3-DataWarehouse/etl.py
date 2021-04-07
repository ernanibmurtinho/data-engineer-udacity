from time import time
import configparser
import psycopg2
import matplotlib.pyplot as plt
import pandas as pd
from sql_queries import copy_table_queries, insert_table_queries, insert_table

config = configparser.ConfigParser()
config.read('dwh.cfg')

def load_staging_tables(cur, conn):
    loadTimes = []
    
    for schema, table, s3path in copy_table_queries:
        SQL_SET_SCHEMA = "SET search_path TO {};".format(schema)
        cur.execute(SQL_SET_SCHEMA)
        SQL_COPY = ("""                    
                    copy {}.{} from {} 
                    credentials 'aws_iam_role={}'
                    region 'us-west-2' format as json 'auto'
                            """).format(schema, table, config.get("S3", s3path), config.get("DWH", "DWH_ROLE_ARN"))

        print("======= LOADING STAGING TABLE: ** {} ** IN SCHEMA ==> {} =======".format(table, schema))
        print(SQL_COPY)

        t0 = time()
        cur.execute(SQL_COPY)
        conn.commit()
        tables = []
        tables.append(table)
        loadTime = time()-t0
        loadTimes.append(loadTime)

        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
    return pd.DataFrame({"table":str(tables), "loadtime_":loadTimes}).set_index('table')


def insert_tables(cur, conn):
    
    insertTimes = []
    
    for (query, table) in zip( insert_table_queries, insert_table ) :
        
        print("======= INSERTING DATA INTO TABLE: ** {} ** =======".format(table))
        t0 = time()
        cur.execute(query)
        conn.commit()
        insertTime = time()-t0
        insertTimes.append(insertTime)
        
        print("=== DONE IN: {0:.2f} sec\n".format(insertTime))
    return pd.DataFrame({"table":insert_tables, "inserttime_":insertTimes}).set_index('table')

    
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    tbStats = load_staging_tables(cur, conn)
    tbStats.plot.bar()
    plt.show()
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
