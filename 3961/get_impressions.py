from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *

# Configure SparkSession (assuming you have Spark installed)
spark = SparkSession.builder.appName("IP Maxmind Processing").getOrCreate()

ip_city = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3961/files/ip_city.csv', header = True)
ip_impressions = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3961/ip_uniqueRecordId/*')

result = ip_city.join(ip_impressions, on = 'ip', how = 'inner')
result = result.groupBy('city').agg(countDistinct('unique_record_id').alias('impressions'))
result.coalesce(1).write.option("header", True).mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3961/city_impressions/')