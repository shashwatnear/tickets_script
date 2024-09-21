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

start = '2024-07-01'
end = '2024-07-10'

dates = get_dates_between(start, end)

result = None
for date in dates:
    df = spark.read.parquet(f's3://staging-near-data-analytics/shashwat/ps-3955/refined/footfall/{date}/*')
    df = df.select('geohash').dropDuplicates()
    if result is None:
        result = df
    else:
        result = result.union(df)

result = result.dropDuplicates()

result.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3955/sample/refined/', header=True)
