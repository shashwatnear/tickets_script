from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("overall").getOrCreate()

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3992/poi_footfall/stay/*/*')
print("Total exposed IDs")
df.agg(countDistinct('ifa')).show()

demog_df = spark.read.csv('s3://near-data-analytics/Ajay/coreSeg.csv', header=True)

df = df.withColumn('date', col('eventDTLocal').substr(1, 10))
temp = df.groupBy('ifa').agg(countDistinct('date').alias('exposed_days'))

print("avg exposed days")
temp.agg(avg('exposed_days')).show()
print("freq 1")
temp.filter(col('exposed_days') == 1).agg(countDistinct('ifa')).show()
print("freq 2")
temp.filter(col('exposed_days') == 2).agg(countDistinct('ifa')).show()
print("freq 3 or greater than 3")
temp.filter((col('exposed_days') == 3) | (col('exposed_days') > 3)).agg(countDistinct('ifa')).show()

df = df.withColumn('coreSeg', explode_outer('coreSeg')).drop_duplicates()
df = df.join(demog_df, on='coreSeg').drop('coreSeg')

print("Total distinct male IDs")
df.filter(col('desc') == 'Male').agg(countDistinct('ifa')).show()
print("Total distinct female IDs")
df.filter(col('desc') == 'Female').agg(countDistinct('ifa')).show()

