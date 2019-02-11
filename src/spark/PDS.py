"""
This is the primary batch processessor class that runs a PySpark job.

This script calculates the daily average stock prices fecthed from AWS S3 (bucket: de-yk-bucket).
Stock prices are stored in directories in the bucket.
For each available date (including non-trading-days), a directory with the naming convention
in the ISO 8601 format YYYY-MM-DD is created.

For dates other than the current trading day in Frankfurt time, where the exchanges live,
a file exists for each hour of the trading day.

These files are named YYYY-MM-DD_BINS_mrkthh.csv, where mkrt is XEUR (ISO 10383 Market Identification Codes)
and hh is the two digit hour indicating which hour of trading the file contains, in 24 hour format.

This application is developed as a part of an Insight data science project.

Author: Yagiz kaymak
February, 2019
"""
import os, sys
from datetime import date, timedelta
import datetime
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import boto3, botocore
import secret
from io import BytesIO as StringIO

# Date format that is used to convert string date to date object
DATE_FORMAT = "%Y-%m-%d"

def main(*argv):
    conf = SparkConf()
    conf.setMaster('spark://ec2-18-235-191-19.compute-1.amazonaws.com:7077')
    conf.setAppName('PDS')
    sc = SparkContext(conf=conf)

    spark = SparkSession.builder.appName('PDS').getOrCreate()

    aws_access_key = secret.secret["aws_access_key"]
    aws_secret_access_key = secret.secret["aws_secret_access_key"]

    # Get S3 client object which provides a low-level functional API for S3
    client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key= aws_secret_access_key)


    try:
        # Get S3 resource for more detailed S3 operations
        s3 = boto3.resource('s3', aws_access_key_id=aws_access_key, aws_secret_access_key= aws_secret_access_key)
    except Exception, e:
        print ("Boto3 connection error: ", e)


    try:
        # Get bucket object
        bucket = s3.Bucket('de-yk-bucket')
    except Exception, e:
        print ("Bucket fetch error: ", e)

    todays_date = datetime.datetime.now().date()
    todays_date_str = todays_date.strftime(DATE_FORMAT)

    # Get all folders for a given date interval.
    # Since everything is already synced so far,
    for folder in perdelta(todays_date, todays_date, timedelta(days=1)):

        # Prefix to get the folder for a specific day
        prefix = "PDS/XETR/"
        prefix = prefix + str(folder) + '/'

        daily_df = pd.DataFrame(columns=['Mnemonic','Date', 'sum','count'])

        # Iterate through the files in the folder for a specific day
        for obj in bucket.objects.filter(Prefix=prefix):
            # csv files with no data are explicitly defined with a size of 136 bytes
            if obj.size != 136 and "XEUR" not in str(obj.key):
                path = "s3n://de-yk-bucket/" + obj.key
                print obj.key
                obj = client.get_object(Bucket='de-yk-bucket', Key=obj.key)

                try:
                    df = pd.read_csv(obj['Body'], error_bad_lines=False)
                except Exception, e:
                    print ("CSV file read error: ", e)

                    try:
                        df = pd.read_csv(obj['Body'], engine='python')
                    except Exception, e:
                        print ("CSV file read error: ", e)
                        continue

                # Group the daily stock trades by 'Stock Mnemonic' and 'Trade Date' and calculate the sum and count of each group
                grouped = df.groupby(['Mnemonic', 'Date'], as_index=False)['EndPrice'].agg(['sum','count']).reset_index()

                df_new = pd.DataFrame(columns = ['Mnemonic', 'Date', 'sum', 'count'])
                df_new['Mnemonic'] = grouped['Mnemonic']
                df_new['Date'] = grouped['Date']
                df_new['sum'] = grouped['EndPrice']['sum']
                df_new['count'] = grouped['EndPrice']['count']

                # Append trades in each file to one dataframe
                daily_df = daily_df.append(df_new)

        df_to_db = pd.DataFrame(columns = ['TradingDate', 'Ticker', 'AvgPrice'])

        # If the daily avervage dataframe is not empty prepare it to be
        # stored in a temp csv file in S3
        if not daily_df.empty:
            aggregation_functions = {'Date': 'first', 'sum': 'sum', 'count': 'sum'}
            res_df = daily_df.groupby('Mnemonic').aggregate(aggregation_functions).reset_index()

            df_to_db['TradingDate'] = res_df['Date']
            df_to_db['Ticker'] = res_df['Mnemonic']

            res_df['sum'] = pd.to_numeric(res_df['sum'], errors='coerce')
            res_df['count'] = pd.to_numeric(res_df['count'], errors='coerce')

            df_to_db['AvgPrice'] = res_df['sum']/res_df['count']

        # Create a temp csv file to temporarily store daily averages in S3
        output_file_name = 'PDS/XETR/DailyAverages/' + str(folder) + '.csv'

        csv_buffer = StringIO()
        df_to_db.to_csv(csv_buffer, index=False)
        s3.Object('de-yk-bucket', output_file_name).put(Body=csv_buffer.getvalue())


def perdelta(start, end, delta):
    """ Function that returns all dates in a given date range (start and end dates are inclusive) """
    curr = start
    while curr <= end:
        yield curr
        curr += delta


if __name__=="__main__":
    main(*sys.argv)
