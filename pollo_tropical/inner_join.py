from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3979_one_year").getOrCreate()

df1 = spark.read.parquet('s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=669a976dfb552b2cc7bca3e6/data_cache/visits/location=b46bed1a3fb1d389723148d557b538d1/*')
'''
{
  "ifa": "cb8656c5-9636-4377-a763-cea53d8c2daf",
  "aspkId": 73197404122,
  "geoHash9": "dhwgjmpbc",
  "run_date": "2024-07-19 12:53:21",
  "near_poi_id": "Pollo Tropical 10190, Hialeah, FL|8372934"
}
'''
df2 = spark.read.csv('s3://near-data-analytics/3995_distance_travelled/669a976dfb552b2cc7bca3e6s/all_ids_combined/first_column_unique.csv.gz')
df2 = df2.withColumnRenamed('_c0', 'ifa')
'''
00193248-a1d7-4958-ab41-8a844ef62923
'''
df3 = df1.join(df2, on = 'ifa', how = 'inner')
df3.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/acm/inner_join_result/")