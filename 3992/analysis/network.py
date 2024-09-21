from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("network").getOrCreate()

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3992/poi_footfall/stay/*/*')

print("network wise Total exposed IDs")
total_ids = df.groupBy('network').agg(countDistinct('ifa')).orderBy('network')
total_ids.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3992/reports/network/total/")

df = df.withColumn('date', col('eventDTLocal').substr(1, 10))
temp = df.groupBy('network', 'ifa').agg(countDistinct('date').alias('exposed_days'))

print("network wise avg exposed days")
avg = temp.groupBy('network').agg(avg('exposed_days')).orderBy('network')
avg.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3992/reports/network/avg/")

print("network wise freq 1")
freq1 = temp.filter(col('exposed_days') == 1).groupBy('network').agg(countDistinct('ifa')).orderBy('network')
freq1.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3992/reports/network/freq1/")

print("network wise freq 2")
freq2 = temp.filter(col('exposed_days') == 2).groupBy('network').agg(countDistinct('ifa')).orderBy('network')
freq2.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3992/reports/network/freq2/")

print("network wise freq 3 or greater than 3")
freq3 = temp.filter((col('exposed_days') == 3) | (col('exposed_days') > 3)).groupBy('network').agg(countDistinct('ifa')).orderBy('network')
freq3.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3992/reports/network/freq3/")

# ######################################################

demog_df = spark.read.csv('s3://near-data-analytics/Ajay/coreSeg.csv', header=True)

df = df.withColumn('coreSeg', explode_outer('coreSeg')).drop_duplicates()
df = df.join(demog_df, on='coreSeg').drop('coreSeg')

print("City wise Total distinct male IDs")
male = df.filter(col('desc') == 'Male').groupBy('network').agg(countDistinct('ifa')).orderBy('network')
male.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3992/reports/network/male/")
# ######################################################
print("City wise Total distinct female IDs")
female = df.filter(col('desc') == 'Female').groupBy('network').agg(countDistinct('ifa')).orderBy('network')
female.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3992/reports/network/female/")
# ######################################################

