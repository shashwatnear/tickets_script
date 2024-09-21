from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash
import hashlib

spark = SparkSession.builder.appName("3932_refined").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

df1 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/final/refined/footfall/*/*')
df2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3932/final/report/report_1/part-00000-65c758c1-1222-4c97-9848-5a7f137c96f1-c000.csv', header = True)
df2 = df2.select('ifa')

df3 = df1.join(df2, on = 'ifa', how = 'left_anti')

df3.write.mode('append').parquet("s3://staging-near-data-analytics/shashwat/ps-3932/final/refined/footfall/combined/")