from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash
import hashlib

spark = SparkSession.builder.appName("smrt_test").getOrCreate()

df1 = spark.read.csv('s3://staging-near-data-analytics/shashwat/smrt/july/reports/profiles_combined/part-00000-2d724d21-b65a-4be6-a625-8e771de9603f-c000.csv', header = True)
df2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/smrt/july/reports/report_2/part-00000-1b53a0d9-51b0-436e-bb1e-92cc78af754f-c000.csv', header = True)
df3 = spark.read.csv('s3://staging-near-data-analytics/shashwat/smrt/july/reports/report_3/part-00000-33ef4c4e-43e8-4c83-83cf-ffe6fb3b9e74-c000.csv', header = True)

result = df3.join(df1, on = 'deviceID', how = 'left')
result = result.join(df2, on = 'deviceID', how = 'left')
result.show(5, False)
result.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/smrt/july/all_combined/", header=True)