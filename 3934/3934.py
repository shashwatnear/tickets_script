from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3934").getOrCreate()

# def get_geohash(lat, lon):
#     a = proximityhash.create_geohash(float(lat), float(lon), 30, 9)
#     return a
# generate_geohash_udf = udf(get_geohash)

# poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3934/files/3934.csv', header = True)
# poi = poi.filter(col('Province') == 'İstanbul')
# poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
# poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
# poi = poi.withColumn('geohash',explode('geohash'))

# start = "2024-05-01"
# end = "2024-05-31"
# country = "TUR"

# dates = get_dates_between(start, end)

# # Footfall report stay
# for date in dates:
#     day = "{:02d}".format(date.day)
#     month = '{:02d}'.format(date.month)
#     year = date.year
#     dest_path = "s3://staging-near-data-analytics/shashwat/ps-3934/data/staypoints_data/footfall/{}/".format(date)

#     data = spark.read.parquet(f"s3://near-datamart/staypoints/version=*/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country={country}/*")
#     data = data.filter(col("hdp_flag")==0)
#     data = data.select(['ifa', data.geoHash9.substr(1, 9).alias('geohash')])
#     ff_df = data.join(poi, on='geohash', how='inner').drop('lat', 'lon', 'geohash')
#     ff_df.write.mode("append").parquet(dest_path)

# print("staypoints done")

# # Footfall report refined
# for date in dates:
#     day = "{:02d}".format(date.day)
#     month = '{:02d}'.format(date.month)
#     year = date.year
#     dest_path = "s3://staging-near-data-analytics/shashwat/ps-3934/data/refined_data/footfall/{}/".format(date)

#     data = spark.read.parquet(f"s3://near-data-warehouse/refined/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country={country}/*")
#     data = data.select(['ifa', data.geoHash9.substr(1, 9).alias('geohash')])
#     ff_df = data.join(poi, on='geohash', how='inner').drop('lat', 'lon', 'geohash')
#     ff_df.write.mode("append").parquet(dest_path)

# print("refined done")

# # Footfall report ubermedia
# for date in dates:
#     day = "{:02d}".format(date.day)
#     month = '{:02d}'.format(date.month)
#     year = date.year
#     dest_path = "s3://staging-near-data-analytics/shashwat/ps-3934/data/uber_data/footfall/{}/".format(date)

#     data = spark.read.parquet(f"s3://near-data-warehouse/refined/dataPartner=ubermedia/year={year}/month={month}/day={day}/hour=*/country={country}/*")
#     data = data.filter((col('src') == 'complementics') | (col('src') == 'irys') | (col('src') == 'predicio') | (col('src') == 'xmode'))
#     data = data.select(['ifa', data.geoHash9.substr(1, 9).alias('geohash')])
#     ff_df = data.join(poi, on='geohash', how='inner').drop('lat', 'lon', 'geohash')
#     ff_df.write.mode("append").parquet(dest_path)

# print("ubermedia done")

# stay = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3934/data/staypoints_data/footfall/*/*')
# ref = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3934/data/refined_data/footfall/*/*')
uber = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3934/data/uber_data/footfall/*/*')

# stay = stay.groupBy('name', 'neighbourhood').agg(countDistinct('ifa').alias('footfall'))
# ref = ref.groupBy('name', 'neighbourhood').agg(countDistinct('ifa').alias('footfall'))
grp = uber.groupBy('name', 'neighbourhood').agg(countDistinct('ifa').alias('footfall'))


# stay.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-3934/poi_footfall/stay/", header=True)
# ref.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-3934/poi_footfall/refined/", header=True)
# uber.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-3934/poi_footfall/uber_media/", header=True)

result = uber.join(grp, on = ['name', 'neighbourhood'], how = 'inner').drop('ifa')

result = result.dropDuplicates(['name', 'neighbourhood'])
result.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-3934/poi_footfall/uber_all/", header=True)
