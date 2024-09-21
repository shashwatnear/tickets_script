from pyspark.sql.functions import *
from datetime import date,timedelta,datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import datetime
from datetime import datetime, timedelta
import requests,json
import pandas as pd
import proximityhash
import Geohash
import geohash
from pyspark.sql import functions as F
import pytz
from pyspark.sql.window import Window
from util import *
import subprocess
import os
import glob

spark = SparkSession.builder.appName("3916_April").getOrCreate()
sc = spark.sparkContext


# #Footfall script
# def convert(lat1,lon1):
#     a = proximityhash.create_geohash(float(lat1),float(lon1),int(50),int(8))
#     return a
# lat_udf = udf(convert, StringType())

# optus = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3916/files/bayer_poi_list.csv', header=True).select('poi_no','name','address','lat','lon','class','City','Province').dropDuplicates()
# optus = optus.where(col('Province').isin(["Istanbul", "Ankara", "Izmir"]))
# optus = optus.withColumn("geohash", lat_udf(col('lat'), col('lon')))
# optus = optus.withColumn('geohash',(split(optus['geohash'],',')))
# optus = optus.withColumn('geohash',explode('geohash'))


# def fileLocations_to_Footfall(start,end,country,destination_path):
#     dates = get_dates_between(start, end)
#     for date in dates:
#         try:
#             day = "{:02d}".format(date.day)
#             month = '{:02d}'.format(date.month)
#             year = date.year 
#             path = "s3://near-data-warehouse/refined/dataPartner=*/year={}/month={}/day={}/hour=*/country={}/*".format(year,month,day,country)
#             print(path)
#             data_footfall = spark.read.parquet(path)
#             data_footfall = data_footfall.select(["ifa","aspkId",data_footfall.geoHash9.substr(1,8).alias("geohash"),"eventDTLocal","coreSeg"])
#             total = data_footfall.join(optus,on='geohash')
#             total.write.mode("append").parquet("s3://staging-near-data-analytics/{}/{}".format(destination_path,date))
#         except Exception as e:
#             print(e)
#     return "success"


# start="2024-04-28"
# end="2024-05-04"
# country="TUR"
# destination_path="shashwat/ps-3916/April_Data/50m/4th_week/"
# fileLocations_to_Footfall_report = fileLocations_to_Footfall(start,end,country,destination_path)
# print(fileLocations_to_Footfall_report)


# ##########################################################################################################################################

# File 1
df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3916/April_Data/50m/4th_week/*/*').withColumnRenamed("Province", "District").withColumnRenamed("City", "Province")
istanbul = df.filter(col('District') == 'Istanbul').dropDuplicates(['District', 'ifa'])
istanbul = istanbul.limit(500)

ankara = df.filter(col('District') == 'Ankara').dropDuplicates(['District', 'ifa'])
ankara = ankara.limit(500)

izmir = df.filter(col('District') == 'Izmir').dropDuplicates(['District', 'ifa'])
izmir = izmir.limit(500)

df = istanbul.union(ankara)
df = df.union(izmir)

df.show(50)

# window_spec = Window.partitionBy("District").orderBy("ifa")
# df = df.withColumn("row_number", row_number().over(window_spec))
# df = df.filter("row_number <= 500")
# df = df.drop('row_number')

# gen = df.withColumn("coreSeg", explode_outer(df.coreSeg))
# gen = gen.withColumn('coreSeg', when(gen.coreSeg == '1', "Male").when(gen.coreSeg == '2', "Female").when(gen.coreSeg == '3', "18-24").when(gen.coreSeg == '4', "25-34").when(gen.coreSeg == '5', "35-44").when(gen.coreSeg == '86', "45-54").when(gen.coreSeg== '7', "Student").when(gen.coreSeg == '8', "Professional").when(gen.coreSeg == '9', "Affluent").when(gen.coreSeg == '10', "Parent").when(gen.coreSeg == '12', "Shopper").when(gen.coreSeg == '13', "Traveller").when(gen.coreSeg == '87', "55+").otherwise(gen.coreSeg))
# gen = gen.filter((gen.coreSeg == "Male")|(gen.coreSeg == "Female")|(gen.coreSeg == "18-24")|(gen.coreSeg == "25-34")|(gen.coreSeg == "35-44")|(gen.coreSeg == "45-54")|(gen.coreSeg == "55+"))
# pivotDF = gen.groupBy("poi_no", "name", 'address', 'lat', 'lon', 'class', 'District', 'Province').pivot("coreSeg").agg(countDistinct("ifa"))

# footfall = df.groupBy("poi_no", "name", 'address', 'lat', 'lon').agg(countDistinct("ifa").alias("Footfall"))

# li = ["poi_no", "name", 'address', 'lat', 'lon']
# result_df = pivotDF.join(footfall, on= li, how='inner')
# result_df = result_df.withColumn("poi_no", result_df["poi_no"].cast("int"))
# result_df = result_df.orderBy("poi_no")
# result_df.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3916/Final/april/50m/4th_week/File_1/")

# # File 4

# df_hcl = df.filter((col('class') == "Bronze") | (col('class') == "Silver") | (col('class') == "Gold") | (col('class') == "Platin"))
# gen = df_hcl.withColumn("coreSeg", explode_outer(df_hcl.coreSeg))
# gen = gen.withColumn('coreSeg', when(gen.coreSeg == '1', "Male").when(gen.coreSeg == '2', "Female").when(gen.coreSeg == '3', "18-24").when(gen.coreSeg == '4', "25-34").when(gen.coreSeg == '5', "35-44").when(gen.coreSeg == '86', "45-54").when(gen.coreSeg== '7', "Student").when(gen.coreSeg == '8', "Professional").when(gen.coreSeg == '9', "Affluent").when(gen.coreSeg == '10', "Parent").when(gen.coreSeg == '12', "Shopper").when(gen.coreSeg == '13', "Traveller").when(gen.coreSeg == '87', "55+").otherwise(gen.coreSeg))
# gen = gen.filter((gen.coreSeg == "Male")|(gen.coreSeg == "Female")|(gen.coreSeg == "18-24")|(gen.coreSeg == "25-34")|(gen.coreSeg == "35-44")|(gen.coreSeg == "45-54")|(gen.coreSeg == "55+"))
# pivotDF = gen.groupBy('class').pivot("coreSeg").agg(countDistinct("ifa"))

# footfall = df_hcl.groupBy("class").agg(countDistinct("ifa").alias("Footfall"))

# result_df = pivotDF.join(footfall, on= 'class', how='inner')

# hcc_coreseg = gen.withColumn('hcp_co', F.lit('hcp'))
# hcc_coreseg = hcc_coreseg.groupBy('hcp_co').pivot("coreSeg").agg(countDistinct("ifa"))
# footfall_count = gen.agg(F.countDistinct("ifa").alias("Footfall")).collect()[0]["Footfall"]
# hcc_coreseg = hcc_coreseg.withColumn('Footfall', F.lit(footfall_count))
# hcc_coreseg = hcc_coreseg.withColumnRenamed('hcp_co', 'class')
# final = result_df.union(hcc_coreseg)
# final.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3916/Final/april/50m/4th_week/File_4/")


# # File 2
# df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3916/April_Data/50m/4th_week/*/*').withColumnRenamed("Province", "District").withColumnRenamed("City", "Province")
# window_spec = Window.partitionBy("District").orderBy("ifa")
# df = df.withColumn("row_number", row_number().over(window_spec))
# df = df.filter("row_number <= 500")
# df = df.drop('row_number')

# df = df.withColumn('date',df.eventDTLocal.substr(1,10))

# window_spec = Window().partitionBy("ifa", "name").orderBy("date")

# # Calculate the visit count for each user and store
# df = df.withColumn("visit_count", F.count("date").over(window_spec))

# # Filter for users who visited a store 3 or more times
# result_df = df.filter("visit_count >= 3").select("ifa", "poi_no", "name", 'address', 'lat', 'lon', 'class', 'District', 'Province').distinct()

# # Group by 'store' and count the distinct users
# final_result = result_df.groupBy("poi_no", "name", 'address', 'lat', 'lon', 'class', 'District', 'Province').agg(F.countDistinct("ifa").alias("Footfall"))
# final_result = final_result.withColumn("poi_no", final_result["poi_no"].cast("int"))
# final_result = final_result.orderBy("poi_no")
# final_result.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3916/Final/april/50m/4th_week/File_2")


# # File 3

# df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3916/April_Data/50m/4th_week/*/*').withColumnRenamed("Province", "District").withColumnRenamed("City", "Province")
# window_spec = Window.partitionBy("District").orderBy("ifa")
# df = df.withColumn("row_number", row_number().over(window_spec))
# df = df.filter("row_number <= 500")
# df = df.drop('row_number')

# df = df.withColumn('date',df.eventDTLocal.substr(1,10))

# window_spec = Window().partitionBy("ifa", "name").orderBy("date")

# # Calculate the visit count for each user and store
# df = df.withColumn("visit_count", F.count("date").over(window_spec))

# # Filter for users who visited a store 3 or more times
# result_df = df.filter("visit_count > 1").select("ifa", "poi_no", "name", 'address', 'lat', 'lon', 'class', 'District', 'Province').distinct()

# # Group by 'store' and count the distinct users
# final_result = result_df.groupBy("poi_no", "name", 'address', 'lat', 'lon', 'class', 'District', 'Province').agg(F.countDistinct("ifa").alias("Footfall"))
# final_result = final_result.withColumn("poi_no", final_result["poi_no"].cast("int"))
# final_result = final_result.orderBy("poi_no")
# final_result.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3916/Final/april/50m/4th_week/File_3")




# 7th apr - 13th apr
# 14th apr - 20th apr
# 21st apr - 27th apr
# 28th apr - 4th may




