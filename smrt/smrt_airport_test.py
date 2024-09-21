from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash
import hashlib

spark = SparkSession.builder.appName("3966_final").getOrCreate()

def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 50, 8)
    return a
generate_geohash_udf = udf(get_geohash)

start = "2024-06-01"
end = "2024-06-07"

dates = get_dates_between(start, end)

poi = spark.read.csv('s3://near-data-analytics/shashwat/smrt/airport_terminal.csv', header = True, sep = '|')
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))
poi.show()

# # Footfall report
# for date in dates:
#     try:
#         day = "{:02d}".format(date.day)
#         month = '{:02d}'.format(date.month)
#         year = date.year

#         data = spark.read.parquet(f"s3://near-datamart/staypoints/version=*/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country=SGP/*")
#         data = data.select(['ifa', data.geoHash9.substr(1,8).alias('geohash')])
#         ff_df = data.join(poi, on='geohash', how='inner').drop('geohash')
#         ff_df.show()
#     except:
#         print(date, " Path does not exists")