from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash
import hashlib
import argparse
# from pyspark.sql.functions import col, year, month, dayofmonth, explode, size, greater_than_equal, less_than

spark = SparkSession.builder.appName("3966_terminal").getOrCreate()

# UDFs
def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 50, 8)
    return a
generate_geohash_udf = udf(get_geohash)

def computeHash(ID):
    m = hashlib.sha1((ID.upper() + "9o8WnUtwdY").encode())
    return m.hexdigest()

sha_udf = udf(computeHash,StringType())

# ################### Kindergarden / Preschool ##################

poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/files/airport_terminal.csv', header = True, sep = '|')
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))
poi.show()

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_gh8/50m/*/*')
df = df.join(poi, on = 'geohash', how = 'inner')
# df = df.select(['ifa', 'eventDTLocal'])
df = df.withColumn('ifa', upper('ifa'))

df = df.withColumn('date', col('eventDTLocal').substr(1, 10))
df = df.groupBy('name_en', 'ifa').agg(countDistinct('date').alias('occurance'))
df = df.withColumn('occurance', col('occurance').cast('int'))
df = df.filter((col('occurance') >= 1) & (col('occurance') < 10)).orderBy(col('occurance'))
df = df.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
df = df.groupBy('name_en').agg(countDistinct('deviceID').alias('people_count'))
df.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3966/phase_2/airport_terminal/',header=True)
