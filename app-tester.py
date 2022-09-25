#!/usr/bin/python3

from configparser import ConfigParser
from datetime import datetime
from pyspark.sql.functions import substring, length, col, expr

import os
import json
import sqlparse

import pandas as pd
import numpy as np

import connection
import conn_warehouse

#--------------------------------------------------------------------
#THIS PYTHON FILE FUNCTION ONLY AS A TESTER FOR SPARK PROCESSING STEP
#WITHOUT NEEDING TO RUN THE WHOLE APPLICATION (app.py)
#--------------------------------------------------------------------

if __name__ == '__main__':

    filetime = datetime.now().strftime('%Y%m%d')
    print(f"[INFO] Service ETL is Starting .....")

    conn_dwh, engine_dwh  = conn_warehouse.conn()
    cursor_dwh = conn_dwh.cursor()

    conf = connection.config('postgresql')
    conn, engine = connection.psql_conn(conf)
    cursor = conn.cursor()

    path_query = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(
            path_query+'query.sql','r'
            ).read(), strip_comments=True).strip()

    conf = connection.config('spark')
    spark = connection.spark_conn(app="Project5",config=conf)

    print(f"[INFO] Service ETL is Running .....")
    df = pd.read_sql(query, engine)

    #spark processing
    SparkDF = spark.createDataFrame(df)
    SparkDF.groupBy(substring("order_date", 0, 7)).sum("order_total").sort("substring(order_date, 0, 7)") \
        .toPandas() \
            .to_csv(f"order-total.csv", index=False)
    print(f"[INFO] Spark processing success .....")
