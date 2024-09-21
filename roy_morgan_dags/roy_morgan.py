import boto3
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from datetime import timedelta, datetime  # Removed import date
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import requests
import json
import pandas as pd
import Geohash
import pyspark.sql.functions as f
import pytz
import hashlib
import argparse
import subprocess


spark = SparkSession.builder \
        .appName("MongoDB to S3") \
        .config("spark.mongodb.input.uri", "mongodb://da-user:7fDT90x0kebO@nv01-prod-common-mongo09.adnear.net/compass.campaigns") \
        .getOrCreate()



def get_date_range(start_date, end_date):
	today = datetime.today().date()
	current_date = start_date
	dates = []
	while current_date <= end_date:
		if current_date >= today:
			break
		dates.append(current_date)
		current_date = current_date + timedelta(days=1)
	return dates

def get_dates_between(start_date,end_date):
	date_format = "%Y-%m-%d"
	start = start_date
	end = end_date
	start_date = datetime.strptime(start, date_format).date()
	end_date = datetime.strptime(end, date_format).date()
	dates = get_date_range(start_date, end_date)
	return dates


def list_datasource_ids(bucket_name, prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')

    datasource_ids = set()
    for common_prefix in response.get('CommonPrefixes', []):
        # Extract datasource_id from the prefix
        datasource_id = common_prefix['Prefix'].split('datasource_id=')[1].split('/')[0]
        datasource_ids.add(int(datasource_id))

    return sorted(list(datasource_ids)) 


#------------------------- normal pixel------------------------
def normal_pixel(start, end, datasource_ids):
    dates = get_dates_between(start, end)
    range_folder = "{}_{}".format(start.replace("-", ""), end.replace("-", ""))
    folder_count_data = []
    for datasource_id in datasource_ids:
        for date in dates:
            try:
                day = "{:02d}".format(date.day)
                month = '{:02d}'.format(date.month)
                year = date.year 
                print("Reading datasource:", datasource_id)
                path = f"s3://near-pixelmanager/nifi/country=AUS/id=d99529d9/datasource_id={datasource_id}/year={year}/month={month}/day={day}/"
                print(path)
                dfp = spark.read.json(path)
                Date = f"{year}-{month}-{day}"
                impression_count = dfp.count()
                folder_count_data.append((Date, datasource_id, impression_count))
                print(f"{datasource_id} of {year} writing completed.")
            except Exception as e:
                print(f"Skipping {path} due to error: {type(e).__name__}: {e}")

    folder_count_df = spark.createDataFrame(folder_count_data, ["Date", "datasource_id", "impression_count"])
    folder_count_df.select("Date", "datasource_id", "impression_count").write.mode('append').parquet(f"s3://staging-near-data-analytics/shashwat/sojern_pixel_counts/Base_data/normal_pixel/{range_folder}/")
            
def process_mongodb():
    spark.conf.set("spark.mongodb.input.uri", "mongodb://da-user:7fDT90x0kebO@nv01-prod-common-mongo09.adnear.net/compass.campaigns")
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
    df_filtered = df.filter((df["country"] == "AUS") & (df["tenantId"] == "d99529d9"))
    df_filtered.coalesce(1).write.mode("overwrite").json("s3://staging-near-data-analytics/shashwat/sojern_pixel_counts/coarse_1/")
    # spark_mongo.stop()





#------------------------- Processing ------------------------
def process(start, end):
    dates = get_dates_between(start, end)
    range_folder = "{}_{}".format(start.replace("-", ""), end.replace("-", ""))
	
    #---------------read universal pixel-----------------
    
    #----------------------read normal pixel------------------------

    df2 = spark.read.parquet(f"s3://staging-near-data-analytics/shashwat/sojern_pixel_counts/Base_data/normal_pixel/{range_folder}/")
    df2 = df2.select("Date", "datasource_id", "impression_count")
    #------------------------read mongo db data --------------------------
    df3 = spark.read.json("s3://staging-near-data-analytics/shashwat/sojern_pixel_counts/coarse_1/")
    df3 = df3.withColumn("datasource_id", col("pixel.pixelTagId"))
    df3 = df3.select("name", "_id", "datasource_id")


    # Join df3 and df2
    df_normal = df2.join(df3, on=["datasource_id"], how="inner").dropDuplicates()
    df_normal.show(10)

    

    #postprocessing after joining for normal
    df_normal = df_normal.select("Date", "name", "_id","impression_count")
    df_normal = df_normal.withColumnRenamed("name", "Campaign Name").withColumnRenamed("_id", "Campaign ID")
    df_normal = df_normal.withColumn("Campaign ID", regexp_replace(col("Campaign ID").cast("string"), "[\[\]{}]", ""))
    df_normal_final = df_normal.select("Date", "Campaign Name", "Campaign ID", "impression_count").dropDuplicates()
    
    
    #combine both
    df_union= df_normal_final.filter((col("Date") >= start) & (col("Date") < end))
    df_union = df_union.orderBy("Date")
    df_union.coalesce(1).write.mode('append').option("header", "true").csv(f"s3://staging-near-data-analytics/shashwat/sojern_pixel_counts/final_output/{range_folder}/",header=True)
    spark.stop()



   


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Enter date format like YYYY-MM-DD')
    parser.add_argument('--start_date', type=str, help='Start date in YYYY-MM-DD format', required=True)
    parser.add_argument('--end_date', type=str, help='End date in YYYY-MM-DD format', required=True)
    args = parser.parse_args()
    start = args.start_date
    end = args.end_date

    
    bucket_name = 'near-pixelmanager'
    prefix = 'nifi/country=AUS/id=d99529d9/'
    datasource_ids = list_datasource_ids(bucket_name, prefix)
    normal_pixel(start, end, datasource_ids)
    process_mongodb()
    process(start, end)