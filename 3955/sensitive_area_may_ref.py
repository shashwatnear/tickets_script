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


spark = SparkSession.builder.appName("ref").getOrCreate()
# spark.conf.set("spark.driver.maxResultSize", "10g")

places_df = spark.read.csv("s3://staging-near-data-analytics/shashwat/ps-3955/files/Sensitive_FamilyPlanning_gh9.csv", header = True)

start = "2024-05-01"
end = "2024-05-31"
country = "USA"
dates = get_dates_between(start, end)

for date in dates:
    day = "{:02d}".format(date.day)
    month = '{:02d}'.format(date.month)
    year = date.year 

    data = spark.read.parquet(f"s3://near-data-warehouse/refined/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country={country}/*")
    data = data.select(['aspkId', data.geoHash9.substr(1,9).alias('geohash')])
    ff_df = data.join(places_df, on='geohash', how='inner')
    ff_df = ff_df.select(['aspkId', 'geohash'])
    ff_df = ff_df.repartition(5)
    print("Done: ", date)
    ff_df.write.mode("overwrite").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3955/refined_may/footfall/{date}/")

for date in dates:
    try:
        ref = spark.read.parquet(f's3://staging-near-data-analytics/shashwat/ps-3955/refined_may/footfall/{date}/*')
        ref = ref.withColumn('date', lit(date))
        ref = ref.groupBy('date').agg(countDistinct('aspkId').alias('ids'))
        ref.write.mode('overwrite').csv(f's3://staging-near-data-analytics/shashwat/ps-3955/intermediate/refined_may/{date}/', header=True)
    except Exception as e:
        print(f"Error processing date {date}: {e}")

# Now read all intermediate files and combine them
intermediate_files = f's3://staging-near-data-analytics/shashwat/ps-3955/intermediate/refined_may/*'
result = spark.read.csv(intermediate_files, header=True)

result.coalesce(1).write.mode('append').csv(f's3://staging-near-data-analytics/shashwat/ps-3955/final/refined_may/', header=True)