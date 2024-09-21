from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import proximityhash
import Geohash
from util import *

spark = SparkSession.builder.appName("3961_IPs").getOrCreate()

# df = spark.read.parquet('s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=65df705d4028b71efd0d6127/data_cache/impressions/*/*')
# ip_uniqueRecordId = df.select('ip', 'unique_record_id')
# ips = df.select('ip').dropDuplicates()

# # ip_uniqueRecordId.write.mode('append').parquet('s3://staging-near-data-analytics/shashwat/ps-3961/ip_uniqueRecordId')
# ips.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3961/ip/")

df = spark.read.parquet('s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=65df705d4028b71efd0d6127/data_cache/impressions/*/*')
ip_city = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3961/files/ip_city.csv', header = True)

result = df.join(ip_city, on = 'ip', how = 'inner')
result = result.filter(col('city') != 'Manchester')
result = result.drop(col('city'))

result.write.mode('append').parquet("s3://staging-near-data-analytics/shashwat/ps-3961/not_manchester_impressions/parquet/")
result.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3961/not_manchester_impressions/csv/")
