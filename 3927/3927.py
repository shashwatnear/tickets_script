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

spark = SparkSession.builder.appName("3927").getOrCreate()
sc = spark.sparkContext

# # for hkg we took 15 days of data
# import datetime
# currentdate = datetime.datetime.now().strftime("%Y-%m-%d")
# print(currentdate)

# from datetime import datetime
# datelist = pd.date_range(end=datetime.today(), periods=100).tolist()

# dates = []
# for i in datelist:
#     res = str(i.date())
#     dates.append(res)


# countries = ['TUR']
# for country in countries:
#     if country == "TUR":
#         for date in dates[-60:]:
#             try:
#                 path = "s3://near-rule-engine-data/rule-input/{}/{}".format(country,date)
#                 print(path) 
#                 data = spark.read.parquet(path)
#                 data_footfall = data.select(["aspkId",'v_date']).dropDuplicates()
#                 data_footfall.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3927/{}/TUR_rule_engg/{}/{}/".format(currentdate,country,date))
#             except Exception as e:
#                 print(e)
#     else:
#         for date in dates[-90:]:
#             try:
#                 path = "s3://near-rule-engine-data/rule-input/{}/{}".format(country,date)
#                 print(path)
#                 data = spark.read.parquet(path)
#                 data_footfall = data.select(["aspkId",'v_date']).dropDuplicates()
#                 data_footfall.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3927/{}/TUR_rule_engg/{}/{}/".format(currentdate,country,date))
#             except Exception as e:
#                 print(e)

# schema1 = StructType([StructField("aspkId", StringType(), False),
#                       StructField("HomeLocation", StringType(), True),
#                       StructField("new_days", StringType(),True) ])

# df = spark.createDataFrame(data = sc.emptyRDD(), schema=schema1)

# tur = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3927/{}/TUR_rule_engg/TUR/*/*".format(currentdate)).dropDuplicates()
# tur = tur.withColumnRenamed('v_date','tur_date')
# tur_cnts = tur.groupBy('aspkId').agg(countDistinct('tur_date').alias('tur_count'))
# tur_min = tur.groupBy('aspkId').agg(min('tur_date').alias('tur_min_date'))                                                    
# tur_cnts_min = tur_cnts.join(tur_min, on='aspkId', how='inner').dropDuplicates()
# tur_max = tur.groupBy('aspkId').agg(max('tur_date').alias('max_date'))
# tur_cnts_min_max = tur_cnts_min.join(tur_max, on='aspkId', how='inner').dropDuplicates()
# tur_fin = tur_cnts_min_max.withColumnRenamed('max_date','tur_max_date').withColumnRenamed('tur_count', 'tur_days')
# tur_fin = tur_fin.withColumn('diff_tur', datediff('tur_max_date', 'tur_min_date')) 

# # countries = ['DEU','FRA','NLD','BEL','AUT']
# countries = ['DEU','FRA','NLD']
# for country in countries:
#     new = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3927/{}/TUR_rule_engg/{}/*/*".format(currentdate,country)).dropDuplicates()
#     new = new.withColumnRenamed('v_date','{}_date'.format(country))
#     new_cnts = new.groupBy('aspkId').agg(countDistinct('{}_date'.format(country)).alias('{}_days'.format(country)))
#     new_min = new.groupBy('aspkId').agg(min('{}_date'.format(country)).alias('{}_min_date'.format(country)))
#     new_cnts_min = new_cnts.join(new_min, on='aspkId', how='inner').dropDuplicates()
#     new_max = new.groupBy('aspkId').agg(max('{}_date'.format(country)).alias('{}_max_date'.format(country)))
#     new_fin = new_cnts_min.join(new_max, on='aspkId', how='inner').dropDuplicates()
#     new_fin = new_fin.withColumn('diff_{}_date'.format(country), datediff('{}_max_date'.format(country), '{}_min_date'.format(country)))
#     data = tur_fin.join(new_fin, on='aspkId', how='inner').dropDuplicates()
#     data1 = data.filter( (col('tur_days')>1) & (col('{}_days'.format(country))>1))
#     data1 = data1.withColumn('HomeLocation', when(col('tur_days') >= col('{}_days'.format(country)), "TUR").otherwise("{}".format(country)))
#     data1 = data1.withColumn('new_days', (col('{}_days'.format(country)) - col('tur_days'))).select('aspkId','HomeLocation','new_days').dropDuplicates()
#     df = df.union(data1)
#     df = df.dropDuplicates()

# df.write.mode('append').parquet("s3://staging-near-data-analytics/shashwat/ps-3927/{}/TUR/New_tourist_set_60days".format(currentdate))

# da = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3927/{}/TUR/New_tourist_set_60days/*".format(currentdate))
# da1 = da.filter(~(col('HomeLocation')=="TUR"))
# df_1 = da1.filter(col('new_days')>=1).withColumnRenamed('HomeLocation','country').filter((col('country').isin(['DEU','FRA','NLD','BEL','AUT'])))
# df_1.write.mode('append').parquet("s3://staging-near-data-analytics/shashwat/ps-3927/{}/countries/New_tourist_set_60days".format(currentdate))

# ####################

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3927/2024-05-23/countries/New_tourist_set_60days/*')
deu = df.filter(col('country') == 'DEU').select('aspkId')
fra = df.filter(col('country') == 'FRA').select('aspkId')
nld = df.filter(col('country') == 'NLD').select('aspkId')

deu.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3927/final_file/deu/')
fra.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3927/final_file/fra/')
nld.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3927/final_file/nld/')

