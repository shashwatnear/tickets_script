from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.sql import SparkSession
import requests,json
import proximityhash
import Geohash
import pandas as pd
import numpy as np
import time
import subprocess
from util import *


spark = SparkSession.builder.appName("3955_stay").getOrCreate()

places_df = spark.read.csv("s3://staging-near-data-analytics/shashwat/ps-3955/files/Sensitive_FamilyPlanning_gh9.csv", header = True)

start = "2024-06-01"
end = "2024-06-05"
country = "USA"
dates = get_dates_between(start, end)

for date in dates:
    day = "{:02d}".format(date.day)
    month = '{:02d}'.format(date.month)
    year = date.year 
    dest_path = f"s3://staging-near-data-analytics/shashwat/ps-3955/temp/stay/footfall/{date}/"
    data = spark.read.parquet(f"s3://near-datamart/staypoints/version=*/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country={country}/*")
    data = data.where(col("hdp_flag") == 0)
    data = data.select(['aspkId', data.geoHash9.substr(1,9).alias('geohash')])
    ff_df = data.join(places_df, on='geohash', how='inner')
    ff_df = ff_df.select(['aspkId', 'geohash'])
    ff_df = ff_df.repartition(5)
    ff_df.write.mode("append").parquet(dest_path)