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
    a = proximityhash.create_geohash(float(lat), float(lon), 50, 8)
    return a
generate_geohash_udf = udf(get_geohash)

df = spark.read.parquet('s3://near-datamart/staypoints/version=*/dataPartner=*/year=2024/month=06/day=*/hour=*/country=SGP/*')
df = df.select(['ifa', df.geoHash9.substr(1,8).alias('geohash'), 'eventDTLocal'])

# ##################

# kinder garden
poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/files/kinder_pre.csv', header = True, sep = '|')
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))
kd = df.join(poi, on = 'geohash', how = 'inner')
kd = kd.select('ifa')
kd.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/kinder_pre/")

# ##################

# primary school
poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/files/primary_school.csv', header = True, sep = '|')
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))
ps = df.join(poi, on = 'geohash', how = 'inner')
ps = ps.select('ifa')
ps.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/primary_school/")

# ##################

# Audience likely to travel overseas
poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/files/airport_terminal.csv', header = True, sep = '|')
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))
tra = df.join(poi, on = 'geohash', how = 'inner')

tra = tra.withColumn('date', col('eventDTLocal').substr(1, 10))
tra = tra.groupBy('name_en', 'ifa').agg(countDistinct('date').alias('occurance'))
tra = tra.withColumn('occurance', col('occurance').cast('int'))
tra = tra.filter((col('occurance') >= 1) & (col('occurance') < 10)).orderBy(col('occurance'))
tra = tra.select('ifa')
tra.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/travel_overseas/")

# ##################

# Malaysian working in SGP
sgp = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3966/footfall_gh8/50m/combined/*")
sgp = sgp.select(['ifa', 'eventDTLocal'])
sgp = sgp.withColumn('date', col('eventDTLocal').substr(1, 10))
sgp = sgp.groupBy('ifa').agg(countDistinct('date').alias('occurance'))
sgp = sgp.withColumn('occurance', col('occurance').cast('int'))
sgp = sgp.filter(col('occurance') >= 10).orderBy(col('occurance'))
sgp.show()

mys = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3966/mys_gh8_staypoints_data1/*/*")
mys = mys.withColumn('date', col('eventDTLocal').substr(1, 10))
mys = mys.groupBy('ifa').agg(countDistinct('date').alias('occurance'))
mys = mys.withColumn('occurance', col('occurance').cast('int'))
mys = mys.filter(col('occurance') >= 10).orderBy(col('occurance'))
mys.show()

result = sgp.join(mys, on = 'ifa', how = 'inner').drop(sgp['occurance']).drop(mys['occurance'])
result = result.dropDuplicates()
result = result.select('ifa')
result.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/malasian_working_in_sgp/")

# ##################

# Expats
hl = spark.read.parquet('s3://near-datamart/homeLocation/version=v1/MasterDB/dataPartner=combined/year=2024/month=06/day=16/country=SGP/*')
refined = spark.read.parquet('s3://near-data-warehouse/refined/dataPartner=*/year=2024/month=06/day=*/hour=*/country=SGP/*').select(['ifa', 'devLanguage'])
not_en_zh_ms_ta = refined.where(~col('devLanguage').isin(['en', 'zh', 'ms', 'ta']))
result = not_en_zh_ms_ta.join(hl, on = 'ifa', how = 'inner')
result = result.select('ifa')
result.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/expats/")

# ##################

# Blue collars
poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/files/industrial_zone.csv', header = True, sep = '|')
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))
bc = df.join(poi, on = 'geohash', how = 'inner')
bc = bc.select('ifa')
bc.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/blue_collars/")