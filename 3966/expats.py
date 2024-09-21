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

spark = SparkSession.builder.appName("hl_sgp").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

def computeHash(ID):
    m = hashlib.sha1((ID.upper() + "9o8WnUtwdY").encode())
    return m.hexdigest()

sha_udf = udf(computeHash,StringType())

hl = spark.read.parquet('s3://near-datamart/homeLocation/version=v1/MasterDB/dataPartner=combined/year=2024/month=06/day=16/country=SGP/*')
df = spark.read.parquet('s3://near-data-warehouse/refined/dataPartner=*/year=2024/month=06/day=*/hour=*/country=SGP/*').select(['ifa', 'devLanguage'])
df = df.where(~col('devLanguage').isin(['en', 'zh', 'ms', 'ta']))

result = df.join(hl, on = 'ifa', how = 'inner')
result = result.select('ifa')
result = result.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
result.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3966/phase_2/expats/")


