from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from util import *
from pyspark.sql.functions import row_number
from pyspark.sql.window import *


spark = SparkSession.builder.appName("3975").getOrCreate()

df1 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3975/lumos_mys/country=MYS/year=2024/month=*/day=*/*', header = True)
df1 = df1.withColumnRenamed('UDID', 'maid')
df2 = spark.read.parquet('s3://near-data-analytics/Kalyan/ps-3928/maid_vs_poi/*')
df3 = df1.join(df2, on = 'maid', how = 'inner')
df3.groupBy('name').agg(countDistinct('maid').alias('cnt')).show()
# df3.coalesce(1).write.mode("append").csv("s3://staging-near-data-analytics/shashwat/ps-3975/segment_maidCount/", header = True)

