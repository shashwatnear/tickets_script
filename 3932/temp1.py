from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash
import hashlib
import argparse

spark = SparkSession.builder.config("spark.driver.maxResultSize", "10g").appName("3932_refined").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

df = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3932/final/report/report_3/part-00000-e38e2623-12c0-4c7f-b3e4-0ef8874fc2fa-c000.csv', header = True)
df = df.withColumn('Footfall', col('Footfall').cast('int'))
df.agg(sum('Footfall')).show()
df.agg(countDistinct('deviceID')).show()

# 1,762,492,230 - Footfall sum2
# unique ids - 7,667,965

# 145,285,389
# 1,995,099