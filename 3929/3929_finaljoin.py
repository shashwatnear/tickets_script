from pyspark.sql import SparkSession
import geohash
from polygon_geohasher.polygon_geohasher import polygon_to_geohashes, geohashes_to_polygon
import json
from shapely.geometry import shape, mapping
import numpy as np
import pyspark.sql.functions as F
import pyspark.sql.types as T
import s3fs
import boto3
from datetime import datetime, timedelta
from util import *

spark = SparkSession.builder.appName("3929").getOrCreate()

last_year_footfall = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3929/poi/final_footfall/')
last_year_footfall = last_year_footfall.withColumn('ifa', upper('ifa'))

last_year_footfall = last_year_footfall.select('ifa').distinct()
last_year_footfall.agg(countDistinct('ifa')).show()

campaign_attr_footfall = spark.read.parquet('s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=6595cef706b1eb29e15a64a6/dimensions_report/dimensions_database/dimesnion_visit_database/dimension_visit_database_new/')
campaign_attr_footfall = campaign_attr_footfall.withColumnRenamed('ifa_visits', 'ifa')
campaign_attr_footfall = campaign_attr_footfall.withColumn('ifa', upper('ifa'))

campaign_attr_footfall = campaign_attr_footfall.select('ifa').distinct()
campaign_attr_footfall.agg(countDistinct('ifa')).show()

# result = campaign_attr_footfall.join(last_year_footfall, on = 'ifa', how = 'left_anti')
# result = result.select('ifa').distinct()
# result.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3929/poi/ids_only_seen_in_campaign/")

# ##################################

# df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3929/poi/ids_only_seen_in_campaign/*')
# df.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3929/poi/final_csv/")

# ##################################

# df = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3929/poi/final_csv/', header = True)

# scale = spark.read.parquet('s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=6595cef706b1eb29e15a64a6/dimensions_report/dimensions_database/observability/id_final_scale/part-00000-b7f42dd3-9b62-4001-b1a9-754fa2a391b3-c000.gz.parquet')
# scale = scale.withColumnRenamed('ifa_visits', 'ifa')
# scale = scale.withColumn('ifa', upper('ifa'))
# scale = scale.withColumn('scale', col('scale').cast('int'))

# result = df.join(scale, on = 'ifa', how = 'inner')
# result = result.select(['ifa', 'scale']).dropDuplicates()
# result = result.withColumn("ifa", explode(array_repeat(col("ifa"), col("scale"))))
# result = result.select('ifa')
# result.agg(sum('ifa')).show()
# result.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3929/final_report/1/")


'''
{
  "ifa_visits": "60abcb7e-51cd-4e09-9aeb-ed70f62afcaa",
  "ifa_impressions": "60abcb7e-51cd-4e09-9aeb-ed70f62afcaa",
  "first_impressions": "2024-02-18",
  "last_impressions": "2024-02-18",
  "dma_visits": "Eugene, OR",
  "dma_geohash": "9pxwv7",
  "geoHash9": "9pxwv7ds0",
  "near_poi_id": "840-61826228",
  "date": "2024-03-24",
  "time_of_day_visits": 18,
  "day_visits": "2024-03-24",
  "day_of_week_visits": "Sunday",
  "weekday_visits": 1,
  "geohash6_visits": "9pxwv7",
  "week_visits": "2024-03-24 - 2024-03-30",
  "geohash5_visits": "9pxwv",
  "state_visits": "Oregon",
  "state_geohash": "9pxwv",
  "first_touch": 35,
  "last_touch": 35,
  "first_visits": "2024-03-24",
  "last_visits": "2024-03-25",
  "length_of_stay": 1
}
'''

# 181
# 179

