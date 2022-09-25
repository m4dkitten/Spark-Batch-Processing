#!/usr/bin/python3

import os
import json
import sqlparse
import pandas as pd
import numpy as np
import connection
import conn_warehouse

from configparser import ConfigParser
from datetime import datetime
from pyspark.sql.functions import substring, length, col, expr

if __name__ == '__main__':
    filetime = datetime.now().strftime('%Y%m%d')
    print(f"[INFO] Service ETL is Starting .....")

    #connect db warehouse
    conn_dwh, engine_dwh  = conn_warehouse.conn()
    cursor_dwh = conn_dwh.cursor()

    #connect db source
    conf = connection.config('postgresql')
    conn, engine = connection.psql_conn(conf)
    cursor = conn.cursor()

    #connect spark
    conf = connection.config('spark')
    spark = connection.spark_conn(app="ETL-Digitalskola",config=conf)

    #query extract db source
    path_query = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(
            path_query+'query.sql','r'
            ).read(), strip_comments=True).strip()

    #query load db warehouse
    query_dwh = sqlparse.format(
        open(
            path_query+'dwh_design.sql','r'
            ).read(), strip_comments=True).strip()

    try:
        print(f"[INFO] Service ETL is Running .....")
        df = pd.read_sql(query, engine)

        #upload local
        path = os.getcwd()
        directory = path+'/'+'local'+'/'
        if not os.path.exists(directory):
            os.makedirs(directory)
        df.to_csv(f"{directory}dim_orders_{filetime}.csv", index=False)
        print(f"[INFO] Upload Data in LOCAL Success .....")

        #insert dwh
        cursor_dwh.execute(query_dwh)
        conn_dwh.commit()
        df.to_sql('dim_orders', engine_dwh, if_exists='append', index=False)
        print(f"[INFO] Update DWH Success .....")

        #spark processing
        SparkDF = spark.createDataFrame(df)
        SparkDF.groupBy(substring("order_date", 0, 7)).sum("order_total").sort("substring(order_date, 0, 7)") \
            .toPandas() \
            .to_csv(f"order-total.csv", index=False)
        print(f"[INFO] Spark processing success .....")

        print(f"[INFO] Service ETL is Success .....")
    except:
        print(f"[INFO] Service ETL is Failed .....")
    

    