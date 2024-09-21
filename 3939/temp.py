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

spark = SparkSession.builder.appName("temp").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# c1 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/parents_at_kinder_pre/', header = True)
# c2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/primary_school/', header = True)
# c3 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/airport_terminal/', header = True)
# c4 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/malaysian/', header = True)
# c5 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/blue_collars/', header = True)

# r1 = c1.join(c2, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')
# r1 = r1.join(c3, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')
# r1 = r1.join(c4, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')
# r1 = r1.join(c5, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')

# r1.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/final/', header = True)

d1 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/final/', header = True)
d2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/expats/', header = True)
r1 = d1.join(d2, on = ['asset', 'category', 'subCategory', 'location', 'lat', 'lon'], how = 'inner')
r1.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3939/celia/final/', header = True)
