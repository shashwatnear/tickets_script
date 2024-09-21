from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3932_final_join").getOrCreate()

df1 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3932/final/report_gh8/50m/report_3/part-00000-94121b6c-d10e-4ee1-9db4-5ea508d64288-c000.csv', header = True)
df1 = df1.withColumnRenamed('Footfall', 'footfall_50m')
df2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3932/final/report_gh8/100m/report_3/part-00000-ec616d63-ca61-463f-81a9-5bb942ea7e6f-c000.csv', header = True)
df2 = df2.withColumnRenamed('Footfall', 'footfall_100m')
df3 = df1.join(df2, on = ['deviceID', 'asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'Month'], how = 'inner')
df3.coalesce(1).write.mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-3932/celia_demo/50m_100m/", header = True)

