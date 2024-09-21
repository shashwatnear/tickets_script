from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3936").getOrCreate()

df = spark.read.parquet('s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=65e22c0e29ba7472dcf64eb2/data_cache/impressions/*/*')
df = df.select('ip').dropDuplicates()
df.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-3936/ip_list/")