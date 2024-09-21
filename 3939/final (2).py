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
from util import *


spark = SparkSession.builder.appName("3939").getOrCreate()

df1 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3932/celia_footfall_stay/part-00000-9679838f-51ba-4a49-8918-f2d8e4f21cec-c000.csv', header = True)
df1 = df1.withColumn('ratio', round(col('ff_250m') / col('ff_50m')))

df2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/final/part-00000-1482cc40-72dc-4c6d-a132-e1711c2fe35c-c000.csv', header = True)

df3 = df2.join(df1, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')
df3 = df3.select(['asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'parents_at_kinder_pre', 'primary_school', 'airport_terminal', 'malaysian', 'blue_collars', 'expats', 'ratio'])
df3 = df3.withColumn('parents_at_kinder_pre', (col('parents_at_kinder_pre') * col('ratio')).cast('int'))\
    .withColumn('primary_school', (col('primary_school') * col('ratio')).cast('int'))\
    .withColumn('airport_terminal', (col('airport_terminal') * col('ratio')).cast('int'))\
    .withColumn('malaysian', (col('malaysian') * col('ratio')).cast('int'))\
    .withColumn('blue_collars', (col('blue_collars') * col('ratio')).cast('int'))\
    .withColumn('expats', (col('expats') * col('ratio')).cast('int'))

df3 = df3.select(['asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'parents_at_kinder_pre', 'primary_school', 'airport_terminal', 'malaysian', 'blue_collars', 'expats'])
df3.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3939/celia/final/", header = True)

