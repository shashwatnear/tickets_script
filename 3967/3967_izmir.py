from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3967").getOrCreate()

def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 30, 9)
    return a
generate_geohash_udf = udf(get_geohash)

poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3967/files/3967.csv', header = True)
poi = poi.filter(col('Province') == 'İzmir')
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))

start = "2024-05-01"
end = "2024-05-31"
country = "TUR"

dates = get_dates_between(start, end)

# Footfall report ubermedia
for date in dates:
    day = "{:02d}".format(date.day)
    month = '{:02d}'.format(date.month)
    year = date.year
    dest_path = "s3://staging-near-data-analytics/shashwat/ps-3967/poi_footfall/may/izmir/{}/".format(date)

    data = spark.read.parquet(f"s3://near-data-warehouse/refined/dataPartner=ubermedia/year={year}/month={month}/day={day}/hour=*/country={country}/*")
    print(data)
    data = data.filter((col('src') == 'complementics') | (col('src') == 'irys') | (col('src') == 'predicio') | (col('src') == 'xmode'))
    data = data.select(['ifa', data.geoHash9.substr(1, 9).alias('geohash')])
    ff_df = data.join(poi, on='geohash', how='inner').drop('geohash')
    ff_df.write.mode("append").parquet(dest_path)

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3967/poi_footfall/may/izmir/*/*')
df = df.groupBy(['name', 'category', 'top_category', 'telephone', 'id', 'neighbourhood', 'sent', 'building_door_no', 'City', 'Province', 'address', 'lon', 'lat']).agg(countDistinct('ifa').alias('FOOTFALL'))
df.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-3967/bayer_may/izmir/", header = True)
