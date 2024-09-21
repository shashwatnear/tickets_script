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
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pyspark.sql.types as T

spark = SparkSession.builder.appName("Tourist cards").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 250, 8)
    return a
generate_geohash_udf = udf(get_geohash)

# location = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3932/final/report/report_3/part-00000-e38e2623-12c0-4c7f-b3e4-0ef8874fc2fa-c000.csv', header = True)
# profile = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3932/final/report/report_1/part-00000-689cf059-2c11-4201-8a60-a864c6957e83-c000.csv', header = True)

# result = location.join(profile, on = 'deviceID')
# result = result.filter(col('Profile') == 'Parents')

# # kinder garden
# poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/files/kinder_pre.csv', header = True, sep = '|')
# poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
# poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
# poi = poi.withColumn('geohash',explode('geohash'))
# poi.show()

# df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3939/refined/SGP/*/*')
# df = df.join(poi, on = 'geohash', how = 'inner').dropDuplicates()
# df = df.select('ifa')
# df = df.withColumn('ifa', upper(col('ifa')))
# df = df.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')

# temp = result.join(df, on = 'deviceID', how = 'inner')
# temp = temp.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('deviceID').alias('kinder_pre'))
# temp.show()


# @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

# # kinder garden
# poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/files/kinder_pre.csv', header = True, sep = '|')
# poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
# poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
# poi = poi.withColumn('geohash',explode('geohash'))
# poi = poi.select('geohash', 'name_en')
# poi.show()

# df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3939/refined/SGP/*/*')
# df = df.join(poi, on = 'geohash', how = 'inner').dropDuplicates()
# df = df.withColumn('ifa', upper(col('ifa')))
# df.write.mode("overwrite").parquet("s3://staging-near-data-analytics/shashwat/ps-3939/footfall/kinder_pre/")
# df = df.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3939/footfall/kinder_pre/*')

footfall = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3932/final/staypoints/footfall_gh8/250m/*/*")
demog_df = spark.read.csv('s3://near-data-analytics/Ajay/coreSeg.csv', header=True)
footfall = footfall.withColumn('ifa', upper('ifa'))
footfall = footfall.withColumn('coreSeg', explode_outer('coreSeg')).drop_duplicates()
footfall = footfall.join(demog_df, on='coreSeg').drop('coreSeg')
footfall = footfall.withColumnRenamed('desc', 'Profile')
footfall = footfall.filter(col('Profile') == 'Parents')

result = footfall.join(df, on = 'ifa', how = 'inner')
result = result.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('ifa').alias('kinder_pre'))
result.show()
result.coalesce(1).write.mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-3939/celia/kinder_pre/", header = True)