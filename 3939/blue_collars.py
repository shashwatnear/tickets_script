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

spark = SparkSession.builder.appName("3939_industrial").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# UDFs
def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 250, 8)
    return a
generate_geohash_udf = udf(get_geohash)

def computeHash(ID):
    m = hashlib.sha1((ID.upper() + "9o8WnUtwdY").encode())
    return m.hexdigest()

sha_udf = udf(computeHash,StringType())

# ################### Kindergarden / Preschool ##################

poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/files/industrial_zone.csv', header = True, sep = '|')
# poi = poi.select(['name_en', 'lat', 'lon'])
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))
# poi = poi.select('name_en', 'geohash')
poi.show()

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3939/refined/SGP/*/*')
df = df.join(poi, on = 'geohash', how = 'inner')
# df = df.select(['ifa', 'name_en']).dropDuplicates()
df = df.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
# df.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3939/blue_collars/parquet/")
df = df.groupBy('name_en').agg(countDistinct('deviceID').alias('people_count'))
df.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3939/blue_collars/csv/',header=True)

