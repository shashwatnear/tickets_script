import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
from functools import reduce
import os
from util import *

spark = SparkSession.builder.appName("attributed footfall").getOrCreate()

start = "2024-01-15"
end = "2024-08-31"

# dates = get_dates_between(start, end)

# # visits
# for date in dates:
#     try:
#         visits = spark.read.parquet(f"s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=6595cef706b1eb29e15a64a6/data_cache/visits/location=f8f80cf9308fa3b35bbead90c5d7353f/date={date}/*")
#         '''
#         {
#         "ifa": "5c64c331-f08d-425f-975d-a112e36602bf",
#         "aspkId": 76148115862,
#         "geoHash9": "9pxwv7dvz",
#         "run_date": "2024-01-15 23:49:31",
#         "near_poi_id": "840-61826228"
#         }
#         '''
#         visits = visits.select(['ifa', 'run_date', 'near_poi_id'])
#         visits.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-4010/visits/{date}/")
#     except:
#         print("Data not for ", date)

# # cross matrix
# for date in dates:
#     try:
#         day = "{:02d}".format(date.day)
#         month = '{:02d}'.format(date.month)
#         year = date.year
#         cross_matrix = spark.read.parquet(f"s3://near-datamart/universal_cross_matrix/compass/linkages/country=USA/tenant_id=4d7bb233/datasource_id=3244/year={year}/month={month}/day={day}/*")
#         '''
#         {
#         "from": "BA9A6360-E625-4D7A-B7E6-D4C96B79CC86",
#         "to": "aKrEkRuhTuzuFJ2zNvF0DrvJTiFU8siwDZBDVHKgI",
#         "from_type": "ifa",
#         "to_type": "ncid"
#         }
#         '''
#         cross_matrix = cross_matrix.withColumnRenamed('from', 'ifa').withColumnRenamed('to', 'ncid')
#         cross_matrix = cross_matrix.select('ifa', "ncid")
#         cross_matrix.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-4010/cross_matrix/{date}/")
#     except:
#         print("Data not for ", date)

# # impressions
# for date in dates:
#     try:
#         impressions = spark.read.parquet(f"s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=6595cef706b1eb29e15a64a6/data_cache/impressions/date={date}/*")
#         '''
#         {
#         "ip": "23.154.81.241",
#         "country": "USA",
#         "datasource_id": 3244,
#         "dmacode": "0",
#         "http_referer": "https://www.example.com",
#         "istts": 1705439002809,
#         "ncid": "mNs3atKlgJGSDu6MM9DHZ6p4kRwc2dG7GdXQitFKA4",
#         "pver": "18cd10ff28f",
#         "state": "on",
#         "ssid": "1",
#         "adid": "",
#         "unique_record_id": "2024-01-17-146"
#         }
#         '''
#         impressions = impressions.filter(col('datasource_id') == 3244)
#         impressions = impressions.select(['ncid', 'istts', 'unique_record_id'])
#         impressions.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-4010/temp_impressions/{date}/")
#     except:
#         print("Data not for ", date)

# cross_matrix = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4010/cross_matrix/*/*')
# impressions = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4010/temp_impressions/*/*')
# joined_df = impressions.join(cross_matrix, on = 'ncid', how = 'inner').dropDuplicates()
# joined_df = joined_df.select(['ifa', 'istts', 'unique_record_id'])
# joined_df.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-4010/impressions/")

# attributed footfall
impressions = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4010/impressions/*')
impressions = impressions.withColumn('impression_date', to_date(substring(col('unique_record_id'), 1, 10), 'yyyy-MM-dd'))
impressions = impressions.withColumn('ifa', upper('ifa'))

visits = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4010/visits/*')
visits = visits.withColumn("visited_date", expr("date_format(run_date, 'yyyy-MM-dd')"))
visits = visits.withColumn('ifa', upper('ifa'))

joined_df = impressions.join(visits, on = 'ifa', how = 'inner').dropDuplicates()

joined_df = joined_df.withColumn("attribution", datediff(col("visited_date"), col("impression_date")))
joined_df = joined_df.where((F.col("attribution") <= 90) & (F.col("attribution") >= 0))
joined_df = joined_df.filter((F.col("visited_date") >= start) & (F.col("visited_date") <= end))
joined_df.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-4010/attributed_footfall/")