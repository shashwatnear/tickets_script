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

spark = SparkSession.builder.appName("3966_combine all files").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

a1 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3966/reports/report_1/part-00000-f4f89b7e-bbe3-4363-9b6b-de5b946d6895-c000.csv', header = True)
a2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3966/phase2_reports/report_1/part-00000-1357e409-0657-4592-b0f1-c68763050467-c000.csv', header = True)
a3 = a1.union(a2)
a3.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3966/combined_reports/profile/")

b1 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3966/reports/report_2/part-00000-1a393855-7ab5-43a1-a39a-1a57a5eace02-c000.csv', header = True)
b2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3966/phase2_reports/report_2/part-00000-05f2ecbd-8c71-41f8-abef-1e92a4034061-c000.csv', header = True)
b3 = b1.union(b2)
b3.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3966/combined_reports/age_gender/")

c1 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3966/reports/report_3/part-00000-b3fe127a-34ea-4eb6-8b1e-c828e2f1a5f9-c000.csv', header = True)
c2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3966/phase2_reports/report_3/part-00000-5fa9f042-2193-4e56-aec4-cc71d19ba197-c000.csv', header = True)
c3 = c1.union(c2)
c3.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3966/combined_reports/location/")

