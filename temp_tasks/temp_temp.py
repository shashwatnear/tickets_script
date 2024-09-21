from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Window
import pyspark.sql.functions as f
from util import *
import proximityhash

spark = SparkSession.builder.appName("cnt").getOrCreate()

df1 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3979/one_year_50m/stay/*/*/*/*')

df1.agg(countDistinct('ifa')).show()
