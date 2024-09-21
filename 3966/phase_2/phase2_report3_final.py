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

# df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_gh8/50m/combined/*')

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

# result1 = df1.join(df, on = 'ifa', how = 'inner').select('asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'ifa', 'eventDTLocal', 'coreSeg', 'Profile')
# result2 = df2.join(df, on = 'ifa', how = 'inner').select('asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'ifa', 'eventDTLocal', 'coreSeg', 'Profile')
# result3 = df3.join(df, on = 'ifa', how = 'inner').select('asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'ifa', 'eventDTLocal', 'coreSeg', 'Profile')
# result4 = df4.join(df, on = 'ifa', how = 'inner').select('asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'ifa', 'eventDTLocal', 'coreSeg', 'Profile')
# result5 = df5.join(df, on = 'ifa', how = 'inner').select('asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'ifa', 'eventDTLocal', 'coreSeg', 'Profile')
# result6 = df6.join(df, on = 'ifa', how = 'inner').select('asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'ifa', 'eventDTLocal', 'coreSeg', 'Profile')


# final_result = result1.union(result2).union(result3).union(result4).union(result5).union(result6)
# final_result = final_result.dropDuplicates()
# final_result.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3966/2024-08-06/footfall_on_phase1_and_phase2/")

# ###########################
# df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/2024-08-06/footfall_on_phase1_and_phase2/*')
# df = df.select('ifa', 'Profile').dropDuplicates()
# df = df.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
# df.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3966/2024-08-06/phase2_profiles/", header = True)

df1 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3966/2024-08-06/phase2_profiles/', header = True)
df2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3966/reports/report_1/part-00000-f4f89b7e-bbe3-4363-9b6b-de5b946d6895-c000.csv', header = True)
df3 = df1.union(df2)
df3.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3966/2024-08-06/phase2_combined/", header = True)

