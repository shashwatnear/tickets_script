from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *

spark = SparkSession.builder.appName("3932_final").getOrCreate()

df1 = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3932/final/refined/footfall_gh8/*/*")
df1 = df1.select(['ifa', 'category', 'location'])

df2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3932/final/report/report_3/sgp_footfall_check.csv', header = True)

df3 = df1.join(df2, on = ['category', 'location'], how = 'inner')
df3 = df3.groupBy('category', 'location').agg(countDistinct('ifa'))
df3 = df3.orderBy('category', 'location')
df3.coalesce(1).write.mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-3932/nikhil_query_gh8/", header=True)