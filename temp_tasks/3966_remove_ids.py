from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3966_remove_ids").getOrCreate()

df_with_geohash = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_gh8_with_geohash/50m/*/*')
df_filtered = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3966/ids_with_multiple_profile/part-00000-0165e997-72f6-4730-8bf5-9bd9401b3ff1-c000.csv', header = True)

df_with_geohash.agg(countDistinct('ifa')).show()
final = df_with_geohash.join(df_filtered, on = 'ifa', how = 'left_anti')
final.agg(countDistinct('ifa')).show()
final.write.mode("append").parquet('s3://staging-near-data-analytics/shashwat/ps-3966/footfall_gh8_with_geohash/50m/combined/')