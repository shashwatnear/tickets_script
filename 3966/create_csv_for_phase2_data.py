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

spark = SparkSession.builder.appName("create_csv_for_phase2_data").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_gh8/50m/combined/*')

# #####
df1 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/blue_collars/*')
df2 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/malasian_working_in_sgp/*')
df3 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/kinder_pre/*')
df4 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/primary_school/*')
df5 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/expats/*')
df6 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/travel_overseas/*')
# #####

result1 = df1.join(df, on = 'ifa', how = 'inner')
result2 = df2.join(df, on = 'ifa', how = 'inner')
result3 = df3.join(df, on = 'ifa', how = 'inner')
result4 = df4.join(df, on = 'ifa', how = 'inner')
result5 = df5.join(df, on = 'ifa', how = 'inner')
result6 = df6.join(df, on = 'ifa', how = 'inner')

# #####

result1 = result1.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('ifa').alias('blue_collars'))
result2 = result2.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('ifa').alias('malaysian'))
result3 = result3.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('ifa').alias('parents_at_kinder_pre'))
result4 = result4.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('ifa').alias('parents_primary_school'))
result5 = result5.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('ifa').alias('expats'))
result6 = result6.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('ifa').alias('travel_overseas'))

# ##### This is different keep it commented everytime ########

# result1.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3966/phase2_all_reports/blue_collars/', header = True)
# result2.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3966/phase2_all_reports/malaysian/', header = True)
# result3.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3966/celia/parents_at_kinder_pre/', header = True)
# result4.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3966/celia/primary_school/', header = True)
# result5.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/expats/', header = True)

# #####

final_result = result1.join(result2, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')
final_result = final_result.join(result3, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')
final_result = final_result.join(result4, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')
final_result = final_result.join(result5, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')
final_result = final_result.join(result6, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')
final_result.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3966/phase2_final_csv/', header = True)

