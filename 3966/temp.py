from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash
import hashlib

spark = SparkSession.builder.appName("3966_removed_ids_with_2_profiles").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

df1 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/footfall_gh8/50m/*/*')
df2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3932/dump/part-00000-a7ce5cfe-341d-497f-b64a-69bc57d43495-c000.csv', header = True)
df2 = df2.select('ifa')

df3 = df1.join(df2, on = 'ifa', how = 'left_anti')

df3.write.mode('append').parquet("s3://staging-near-data-analytics/shashwat/ps-3932/footfall_gh8/50m/combined/")