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


spark = SparkSession.builder.appName("3955").getOrCreate()

start = "2024-01-01"
end = "2024-07-11"

dates = get_dates_between(start, end)

for date in dates:
    try:
        ref = spark.read.parquet(f's3://staging-near-data-analytics/shashwat/ps-3955/rule/footfall/{date}/*')
        ref = ref.withColumn('date', lit(date))
        ref = ref.groupBy('date').agg(countDistinct('aspkId').alias('ids'))
        ref.write.mode('overwrite').csv(f's3://staging-near-data-analytics/shashwat/ps-3955/intermediate/rule/{date}/', header=True)
    except Exception as e:
        print(f"Error processing date {date}: {e}")

# Now read all intermediate files and combine them
intermediate_files = f's3://staging-near-data-analytics/shashwat/ps-3955/intermediate/rule/*'
result = spark.read.csv(intermediate_files, header=True)

result.coalesce(1).write.mode('append').csv(f's3://staging-near-data-analytics/shashwat/ps-3955/final/rule/', header=True)

spark.stop()