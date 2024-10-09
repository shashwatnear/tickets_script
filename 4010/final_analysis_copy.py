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

spark = SparkSession.builder.appName("4010").getOrCreate()

# last_year_footfall = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4010/last_year_footfall_gh9_stay/*/*')
# last_year_footfall = last_year_footfall.withColumn('ifa', upper('ifa'))

# attributed_footfall_in_campaign = spark.read.parquet('s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=6595cef706b1eb29e15a64a6/dimensions_report/dimensions_database/dimension_visit_database/final_visit_data/*')
# attributed_footfall_in_campaign = attributed_footfall_in_campaign.withColumnRenamed('ifa_visits', 'ifa')
# attributed_footfall_in_campaign = attributed_footfall_in_campaign.withColumn('ifa', upper('ifa'))

# print("last year footfall")
# last_year_footfall.agg(countDistinct('ifa')).show()
# print("attributed footfall during campaign")
# attributed_footfall_in_campaign.agg(countDistinct('ifa')).show()

# result = attributed_footfall_in_campaign.join(last_year_footfall, on = 'ifa', how = 'left_anti')
# result = result.select('ifa', 'day_visits').distinct()
# result.coalesce(1).write.option("header",True).mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-4010/ids_seen_only_in_campaign/")

# df = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-4010/ids_seen_only_in_campaign/', header = True)

# scale = spark.read.parquet('s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=6595cef706b1eb29e15a64a6/dimensions_report/dimensions_database/observability/id_final_scale/*')
# scale = scale.withColumnRenamed('ifa_visits', 'ifa')
# scale = scale.withColumn('ifa', upper('ifa'))
# scale = scale.withColumn('scale', col('scale').cast('int'))

# result = df.join(scale, on = 'ifa', how = 'inner')
# print(result.select('ifa').distinct().count())
# result = result.select(['ifa', 'scale']).dropDuplicates()
# result = result.withColumn("ifa", explode(array_repeat(col("ifa"), col("scale"))))
# result = result.select('ifa')
# print(result.select('ifa').count())
# result.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4010/final_report/")

# #####################################

attributed_footfall_in_campaign = spark.read.parquet('s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=6595cef706b1eb29e15a64a6/dimensions_report/dimensions_database/dimension_visit_database/final_visit_data/*')

scale = spark.read.parquet('s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=6595cef706b1eb29e15a64a6/dimensions_report/dimensions_database/observability/id_final_scale/*')
scale = scale.withColumn('scale', col('scale').cast('int'))

result = attributed_footfall_in_campaign.join(scale, on = ['ifa_visits', 'day_visits'], how = 'inner')
result = result.agg(sum('scale').alias('extrapolated_number'))
result.coalesce(1).write.option("header",True).mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-4010/extrapolated_number/")
