from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.sql import SparkSession
import proximityhash
import Geohash
import pandas as pd
import numpy as np
import hashlib

spark = SparkSession.builder.appName("Celia").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

def computeHash(ID):
    m = hashlib.sha1((ID.upper() + "9o8WnUtwdY").encode())
    return m.hexdigest()

sha_udf = udf(computeHash,StringType())

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/final/staypoints/footfall_gh8/50m/*/*')
df = df.select(['asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'ifa'])
df = df.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')

# #####

# df1 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3939/reports/blue_collars/parquet/*')

# df2 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3939/reports/malaysian/parquet/*')
# df2 = df2.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')

# df3 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3939/reports/parents_at_kinder_pre/parquet/*')
# df4 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3939/reports/primary_school/parquet/*')
df5 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3939/reports/expats/parquet/*')
# #####

# result1 = df1.join(df, on = 'deviceID', how = 'inner')
# result2 = df2.join(df, on = 'deviceID', how = 'inner')
# result3 = df3.join(df, on = 'deviceID', how = 'inner')
# result4 = df4.join(df, on = 'deviceID', how = 'inner')
result5 = df5.join(df, on = 'deviceID', how = 'inner')

# #####

# result1 = result1.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('deviceID').alias('blue_collars'))
# result2 = result2.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('deviceID').alias('malaysian'))
# result3 = result3.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('deviceID').alias('parents_at_kinder_pre'))
# result4 = result4.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('deviceID').alias('primary_school'))
result5 = result5.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('deviceID').alias('expats'))

# #####

# result1.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/blue_collars/', header = True)
# result2.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/malaysian/', header = True)
# result3.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/parents_at_kinder_pre/', header = True)
# result4.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/primary_school/', header = True)
result5.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/expats/', header = True)
