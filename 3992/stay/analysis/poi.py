from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("network").getOrCreate()

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3992/double_radius/poi_footfall/stay/*/*')
all = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3992/files/3992.csv', header = True)
# all.agg(countDistinct('poi_name')).show()
# 1,692

print("Total impressions IDs")
total_impressions = df.groupBy('lat', 'lon').agg(count('ifa').alias('total_impressions'))

print("Total exposed IDs")
total_ids = df.groupBy('lat', 'lon').agg(countDistinct('ifa').alias('unique_ids'))
# total_ids.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3992/reports/poi/total/")





df = df.withColumn('date', col('eventDTLocal').substr(1, 10))
temp = df.groupBy('lat', 'lon','ifa').agg(countDistinct('date').alias('exposed_days'))

print("network wise avg exposed days")
avg = temp.groupBy('lat', 'lon').agg(avg('exposed_days').alias('avg_freq'))
# avg.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3992/reports/poi/avg/")
# ######################################################
print("network wise freq 1")
freq1 = temp.filter(col('exposed_days') == 1).groupBy('lat', 'lon').agg(countDistinct('ifa').alias('freq_1'))
# freq1.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3992/reports/poi/freq1/")
# ######################################################
print("network wise freq 2")
freq2 = temp.filter(col('exposed_days') == 2).groupBy('lat', 'lon').agg(countDistinct('ifa').alias('freq_2'))
# freq2.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3992/reports/poi/freq2/")
# ######################################################
print("network wise freq 3 or greater than 3")
freq3 = temp.filter((col('exposed_days') == 3) | (col('exposed_days') > 3)).groupBy('lat', 'lon').agg(countDistinct('ifa').alias('freq_3'))
# freq3.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3992/reports/poi/freq3/")

final = all.join(total_impressions, on = ['lat', 'lon'], how = 'left')
final = final.join(total_ids, on = ['lat', 'lon'], how = 'left')
final = final.join(avg, on = ['lat', 'lon'], how = 'left')
final = final.join(freq1, on = ['lat', 'lon'], how = 'left')
final = final.join(freq2, on = ['lat', 'lon'], how = 'left')
final = final.join(freq3, on = ['lat', 'lon'], how = 'left')



# ######################################################

demog_df = spark.read.csv('s3://near-data-analytics/Ajay/coreSeg.csv', header=True)

df = df.withColumn('coreSeg', explode_outer('coreSeg')).drop_duplicates()
df = df.join(demog_df, on='coreSeg').drop('coreSeg')

print("City wise Total distinct male IDs")
male = df.filter(col('desc') == 'Male').groupBy('lat', 'lon').agg(countDistinct('ifa').alias('male'))
# male.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3992/reports/poi/male/")
# ######################################################
print("City wise Total distinct female IDs")
female = df.filter(col('desc') == 'Female').groupBy('lat', 'lon').agg(countDistinct('ifa').alias('female'))
# female.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3992/reports/poi/female/")
# ######################################################

final = final.join(male, on = ['lat', 'lon'], how = 'left')
final = final.join(female, on = ['lat', 'lon'], how = 'left')

final.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/double_radius/ps-3992/reports/poi/")

