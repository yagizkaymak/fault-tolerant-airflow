"""
This script creates a directed acyclic graph (DAG) for workflow scheduling
used by Airflow workflow scheduler.
The DAG in this script automates a datapipeline as follows
'Deutsche XETR Public Dataset' -> AWS S3 -> Spark Batch processing -> PostgreSQL

This DAG is run everyday by Airflow.

Deutsche XETR Public Dataset is daily synced with a bucket in S3.
Then, a remote Spark batch-processing job is performed as a PySpark job.
The result is temporarily written to S3 and detected by an S3 file sensor
when the file is created. Finally, the file content is stored in PostgreSQL
to be visualized by a Dash webapp.

This application is developed as a part of an Insight data science project.

Author: Yagiz kaymak
February, 2019
"""


import sys, os, boto
import datetime as dt
from datetime import date, timedelta
import psycopg2
import pandas as pd
import fnmatch, re
from sqlalchemy import create_engine
import secret
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import FileSensor
from airflow.operators.sensors import S3KeySensor
from airflow.models import Variable
import airflow.operators
import boto3, botocore

DATE_FORMAT = "%Y-%m-%d"

def conn_db():
    """ Returns database connection object """
    try:
        host = secret.secret["PG_HOST"]
        database = 'pds_db'
        user = secret.secret["PG_USER"]
        password = secret.secret["PG_PSWD"]

        conn_string = 'postgresql://' + str(user) + ':' + str(password) + '@' + str(host) + ':5432/' + str(database)

        engine = create_engine(conn_string)
    except (Exception, psycopg2.Error) as error :
        print ("Error while creating DB engine", error)

    return engine

# Default arguments of DAG that will be created.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2019, 2, 9, 00, 00, 00),
    'email': ['yagiz.kaymak@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

# Creating a DAG that daily runs
dag = DAG('PDS_DAG', default_args=default_args, schedule_interval='@daily')


todays_date = dt.datetime.now().date()
todays_date_str = todays_date.strftime(DATE_FORMAT)

filename = str(todays_date_str) + '.csv'

def store_to_db(**kwargs):
    """ This method stores the temporary csv file in S3 to PostgreSQL """
    engine = conn_db()
    con = engine.raw_connection()

    aws_access_key = secret.secret["aws_access_key"]
    aws_secret_access_key = secret.secret["aws_secret_access_key"]

    client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key= aws_secret_access_key) #low-level functional API

    objectKey = 'PDS/XETR/DailyAverages/' + str(todays_date_str) + '.csv'

    obj = client.get_object(Bucket='de-yk-bucket', Key=objectKey)

    sql_command = """SELECT * FROM avg_xetr WHERE "TradingDate" = '{}' LIMIT 1;""".format(todays_date_str)
    match_df = pd.read_sql(sql_command, con)

    # If the daily average stock prices
    if match_df.empty:
        print "A new dataset will be written to DB. DATE: " + str(todays_date_str)

        try:
            df = pd.read_csv(obj['Body'], error_bad_lines=False)
            print "Inserting a dataframe with a shape of " + str(df.shape) + ". Date is: " + str(todays_date_str)
            df.to_sql('avg_xetr', con=engine, index=False, if_exists='append')
        except (IOError, psycopg2.Error), error :
            print ("Error inserting dataframe to DB! ", error)
    else:
        print "Data already exists in the DB! Skipping..."


srcDir = '/home/ubuntu/fault-tolerant-airflow/src/spark'

# Command to run remote spark batch processing
cmd = 'ssh ubuntu@ec2-18-235-191-19.compute-1.amazonaws.com spark-submit' + ' ' + srcDir + 'PDS.py --master ec2-18-235-191-19.compute-1.amazonaws.com --deploy-mode=cluster'

objectKey = 's3n://de-yk-bucket/PDS/XETR/DailyAverages/' + str(todays_date_str) + '.csv'


# Bash operator that synchronizes the Deutsche XETR Public Dataset with my bucket stored in S3
s3_ingest_opr = BashOperator(task_id='s3_ingest', bash_command='aws s3 sync s3://deutsche-boerse-xetra-pds s3://de-yk-bucket/PDS/XETR/ ', dag=dag)

# Remote batch processing operator that calculates the daily averages of stock prices
spark_batch_opr = BashOperator(task_id='spark_batch', bash_command=cmd, dag=dag)

# S3 file sensor operator that senses the temporarily created csv file in S3
s3_file_sensor_opr = S3KeySensor(
    task_id='s3_file_sensor',
    poke_interval=60,
    timeout=10,
    soft_fail=True,
    bucket_key=objectKey,
    bucket_name=None,
    dag=dag)

# Store to DB operator that stores the calculated daily average prices in PostgreSQL
store_to_db_opr = PythonOperator(task_id = 'store_to_db', provide_context=True, python_callable=store_to_db, dag=dag)


# Create dependencies for the DAG
s3_ingest_opr >> spark_batch_opr >> s3_file_sensor_opr >> store_to_db_opr
