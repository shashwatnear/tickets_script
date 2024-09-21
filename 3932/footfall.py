from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3932_final_join").getOrCreate()

df1 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/final/staypoints/footfall_gh8/50m/*/*')
df2 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/final/staypoints/footfall_gh8/100m/*/*')
df3 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/final/staypoints/footfall_gh8/250m/*/*')
df4 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/final/staypoints/footfall_gh8/500m/*/*')

df1 = df1.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('ifa').alias('ff_50m'))
df2 = df2.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('ifa').alias('ff_100m'))
df3 = df3.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('ifa').alias('ff_250m'))
df4 = df4.groupBy('asset', 'category', 'subCategory', 'location', 'lat', 'lon').agg(countDistinct('ifa').alias('ff_500m'))

result = df1.join(df2, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')
result = result.join(df3, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')
result = result.join(df4, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')
result = result.orderBy('asset')
result.coalesce(1).write.mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-3932/celia_footfall_stay_500/", header = True)


# df1 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3932/celia_footfall_stay/*', header = True)
# df2 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/final/staypoints/footfall_gh8/500m/*/*')
# result = df1.join(df2, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')
# result = result.orderBy('asset')
# result.coalesce(1).write.mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-3932/celia_footfall_stay_500/", header = True)



