from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("4022").getOrCreate()

def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 30, 8)
    return a
generate_geohash_udf = udf(get_geohash)

poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-4022/files/4022.csv', header = True)
poi = poi.filter(col('Province').isin(['İstanbul', 'İzmir', 'Ankara', 'Antalya']))
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))

start = "2024-07-01"
end = "2024-07-31"
country = "TUR"

dates = get_dates_between(start, end)

# Footfall report stay
for date in dates:
    day = "{:02d}".format(date.day)
    month = '{:02d}'.format(date.month)
    year = date.year
    dest_path = "s3://staging-near-data-analytics/shashwat/ps-4022/stay_gh8/july/poi_footfall/{}/".format(date)

    data = spark.read.parquet(f"s3://near-datamart/staypoints/version=*/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country=TUR/*")
    data = data.filter(col('hdp_flag') == 0)
    data = data.select(['ifa', data.geoHash9.substr(1, 8).alias('geohash')])
    ff_df = data.join(poi, on='geohash', how='inner').drop('geohash')
    ff_df.write.mode("append").parquet(dest_path)

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4022/stay_gh8/july/poi_footfall/*/*')
df = df.groupBy(['name', 'category', 'top_category', 'telephone', 'id', 'neighbourhood', 'sent', 'building_door_no', 'City', 'Province', 'address', 'lon', 'lat']).agg(countDistinct('ifa').alias('FOOTFALL_JULY'))
df = df.orderBy(col('Province'))
df.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-4022/stay_gh8/july/report/", header = True)
