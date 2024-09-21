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

# UDFs
def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 50, 8)
    return a
generate_geohash_udf = udf(get_geohash)

def computeHash(ID):
    m = hashlib.sha1((ID.upper() + "9o8WnUtwdY").encode())
    return m.hexdigest()

sha_udf = udf(computeHash,StringType())

# -------------------------------------------------------------

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_gh8/50m/combined/*')

df1 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/blue_collars/*')
df1 = df1.withColumn('Profile', lit('blue_collars'))

df2 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/malasian_working_in_sgp/*')
df2 = df2.withColumn('Profile', lit('malasian_working_in_sgp'))

df3 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/kinder_pre/*')
df3 = df3.withColumn('Profile', lit('parents_at_kinder_pre'))

df4 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/primary_school/*')
df4 = df4.withColumn('Profile', lit('parents_at_primary_school'))

df5 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/expats/*')
df5 = df5.withColumn('Profile', lit('expats'))

df6 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase_2_locations/travel_overseas/*')
df6 = df6.withColumn('Profile', lit('travel_overseas'))

result1 = df1.join(df, on = 'ifa', how = 'inner').select('asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'ifa', 'eventDTLocal', 'coreSeg', 'Profile')
result2 = df2.join(df, on = 'ifa', how = 'inner').select('asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'ifa', 'eventDTLocal', 'coreSeg', 'Profile')
result3 = df3.join(df, on = 'ifa', how = 'inner').select('asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'ifa', 'eventDTLocal', 'coreSeg', 'Profile')
result4 = df4.join(df, on = 'ifa', how = 'inner').select('asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'ifa', 'eventDTLocal', 'coreSeg', 'Profile')
result5 = df5.join(df, on = 'ifa', how = 'inner').select('asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'ifa', 'eventDTLocal', 'coreSeg', 'Profile')
result6 = df6.join(df, on = 'ifa', how = 'inner').select('asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'ifa', 'eventDTLocal', 'coreSeg', 'Profile')


final_result = result1.union(result2).union(result3).union(result4).union(result5).union(result6)
final_result = final_result.dropDuplicates()
final_result.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3966/2024-08-06/footfall_on_phase1_and_phase2/")

# ###########################
df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/2024-08-06/footfall_on_phase1_and_phase2/*')
demog_df = spark.read.csv('s3://near-data-analytics/Ajay/coreSeg.csv', header=True)

df = df.withColumn('ifa', upper(col('ifa')))
result = df.select(['ifa', 'coreSeg'])
result = result.withColumn('coreSeg', explode_outer(col('coreSeg')))

result = result.join(demog_df, on='coreSeg').drop('coreSeg')

result = result.withColumnRenamed('desc', 'Profile')

result = result.select(['ifa', 'Profile'])

profiles = result.filter(col('Profile').isin('Affluents', 'Students', 'Shoppers', 'Parents', 'Professionals'))

pro = result.filter(col('Profile') == 'Professionals')

# Early retirement: join Professionals with age 45-54
ff = result.filter(col('Profile') == '45-54')
early_retirement = pro.join(ff, on='ifa', how='inner').select('ifa').withColumn('Profile', lit('Early_Retirement'))

# Retirement: join Professionals with age 55+
fifty_five_plus = result.filter(col('Profile') == '55+')
retirement = pro.join(fifty_five_plus, on='ifa', how='inner').select('ifa').withColumn('Profile', lit('Retirement_State'))

# Combine all profiles and remove duplicates
final = profiles.union(early_retirement).union(retirement).dropDuplicates()
final = final.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
final.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3966/phase2_reports/report_1/", header = True)

#########################
# report 2
df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase1_and_phase2/*')
demog_df = spark.read.csv('s3://near-data-analytics/Ajay/coreSeg.csv', header=True)
df = df.withColumn('ifa', upper('ifa'))
result = df.select(['ifa','coreSeg']).dropDuplicates()
result = result.withColumn('coreSeg', explode_outer('coreSeg')).drop_duplicates()
result = result.join(demog_df, on='coreSeg').drop('coreSeg')
result = result.withColumnRenamed('desc', 'Profile')
result = result.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
result = result.select(['deviceID', 'Profile'])

gender = result.filter((col('Profile') == 'Male') | (col('Profile') == 'Female'))
gender = gender.withColumnRenamed('Profile', 'Gender')

age = result.filter((col('Profile') == '18-24') | (col('Profile') == '25-34') | (col('Profile') == '35-44') | (col('Profile') == '45-54') | (col('Profile') == '55+'))
age = age.withColumnRenamed('Profile', 'Age')

result = gender.join(age, on = 'deviceID', how = 'inner')
result.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3966/phase2_reports/report_2/", header=True)

# #########################
# report 3
df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_on_phase1_and_phase2/*')
df = df.withColumn('Month', lit('June'))
df = df.withColumn("hour", substring(col("eventDTLocal"), 12, 2)).drop(col('eventDTLocal'))
df = df.groupBy('ifa', 'asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'Month').agg(count('hour').alias('Footfall'))
df = df.withColumn('Footfall', col('Footfall').cast('int'))
df = df.filter(col('Footfall') < 151)
df = df.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
df = df.select(['deviceID', 'asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'Month', 'Footfall'])
df.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3966/phase2_reports/report_3/", header=True)
