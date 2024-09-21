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
poi = poi.filter(col('Province').isin(['İzmir', 'Ankara', 'İstanbul']))
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))

start = "2024-07-01"
end = "2024-07-31"
country = "TUR"

dates = get_dates_between(start, end)

# Footfall report ubermedia
for date in dates:
    day = "{:02d}".format(date.day)
    month = '{:02d}'.format(date.month)
    year = date.year

    data = spark.read.parquet(f"s3://near-data-warehouse/refined/dataPartner=ubermedia/year={year}/month={month}/day={day}/hour=*/country={country}/*")
    print(data)
    data = data.filter((col('src') == 'complementics') | (col('src') == 'irys') | (col('src') == 'predicio') | (col('src') == 'xmode'))
    data = data.select(['ifa', data.geoHash9.substr(1, 9).alias('geohash')])
    ff_df = data.join(poi, on='geohash', how='inner').drop('geohash')
    ff_df.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3967/bayer_july/poi_footfall/{}/".format(date))

# #############################

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3967/bayer_july/poi_footfall/*/*')

istanbul = df.filter(col('Province') == 'İstanbul')
ankara = df.filter(col('Province') == 'Ankara')
izmir = df.filter(col('Province') == 'İzmir')

istanbul = istanbul.groupBy(['name', 'category', 'top_category', 'telephone', 'id', 'neighbourhood', 'sent', 'building_door_no', 'City', 'Province', 'address', 'lon', 'lat']).agg(countDistinct('ifa').alias('FOOTFALL'))
ankara = ankara.groupBy(['name', 'category', 'top_category', 'telephone', 'id', 'neighbourhood', 'sent', 'building_door_no', 'City', 'Province', 'address', 'lon', 'lat']).agg(countDistinct('ifa').alias('FOOTFALL'))
izmir = izmir.groupBy(['name', 'category', 'top_category', 'telephone', 'id', 'neighbourhood', 'sent', 'building_door_no', 'City', 'Province', 'address', 'lon', 'lat']).agg(countDistinct('ifa').alias('FOOTFALL'))

istanbul.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-3967/bayer_july/reports/instanbul/", header = True)
ankara.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-3967/bayer_july/reports/ankara/", header = True)
izmir.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-3967/bayer_july/reports/izmir/", header = True)
