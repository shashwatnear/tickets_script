from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3967").getOrCreate()

# df1 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3967/bayer_may/ankara/part-00000-dea0be67-2539-4c5c-b7d6-b779bfccef3d-c000.csv', header = True)
# df1 = df1.withColumnRenamed('FOOTFALL', 'FOOTFALL_MAY')
# df2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3967/bayer_june/ankara/part-00000-dcf1946d-6e1d-4477-9685-809995d916d0-c000.csv', header = True)
# df2 = df2.withColumnRenamed('FOOTFALL', 'FOOTFALL_JUNE')
# df_f1 = df1.join(df2, on = ['id'], how = 'full').drop(df2['name']).drop(df2['category']).drop(df2['top_category']).drop(df2['telephone']).drop(df2['neighbourhood']).drop(df2['sent']).drop(df2['building_door_no']).drop(df2['City']).drop(df2['Province']).drop(df2['address']).drop(df2['lat']).drop(df2['lon'])
# df_f1 = df_f1.select('name', 'category', 'top_category', 'telephone', 'id', 'neighbourhood', 'sent', 'building_door_no', 'City', 'Province', 'address', 'lon', 'lat', 'FOOTFALL_MAY', 'FOOTFALL_JUNE')
# df_f1 = df_f1.withColumn("FOOTFALL_MAY", coalesce(df1["FOOTFALL_MAY"], lit(0))).withColumn("FOOTFALL_JUNE", coalesce(df2["FOOTFALL_JUNE"], lit(0)))

df3 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3967/bayer_may/izmir/part-00000-00f51158-b670-4632-9e05-3c5d6c49794e-c000.csv', header = True)
df3 = df3.withColumnRenamed('FOOTFALL', 'FOOTFALL_MAY')
df4 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3967/bayer_june/izmir/part-00000-13d8ad06-860e-4f0b-bf35-bb41f7719b5c-c000.csv', header = True)
df4 = df4.withColumnRenamed('FOOTFALL', 'FOOTFALL_JUNE')
df_f2 = df3.join(df4, on = ['id'], how = 'full').drop(df3['name']).drop(df4['category']).drop(df4['top_category']).drop(df4['telephone']).drop(df4['neighbourhood']).drop(df4['sent']).drop(df4['building_door_no']).drop(df4['City']).drop(df4['Province']).drop(df4['address']).drop(df4['lat']).drop(df4['lon'])
df_f2 = df_f2.select('name', 'category', 'top_category', 'telephone', 'id', 'neighbourhood', 'sent', 'building_door_no', 'City', 'Province', 'address', 'lon', 'lat', 'FOOTFALL_MAY', 'FOOTFALL_JUNE')
df_f2 = df_f2.withColumn("FOOTFALL_MAY", coalesce(df3["FOOTFALL_MAY"], lit(0))).withColumn("FOOTFALL_JUNE", coalesce(df4["FOOTFALL_JUNE"], lit(0)))

# df_f1.coalesce(1).write.mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-3967/may_vs_june/ankara/", header = True)
df_f2.coalesce(1).write.mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-3967/may_vs_june/izmir/", header = True)