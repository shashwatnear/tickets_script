from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3992_21st_to_30th").getOrCreate()

def get_geohash(lat, lon, radius):
    a = proximityhash.create_geohash(float(lat), float(lon), int(radius), 9)
    return a
generate_geohash_udf = udf(get_geohash)

poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3992/files/3992_21_30.csv', header = True)
# poi = poi.filter(col('Province').isin(['İzmir', 'Ankara', 'İstanbul']))
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon'), col('radius')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))

start = "2024-07-21"
end = "2024-07-30"
country = "TUR"

dates = get_dates_between(start, end)

# Footfall report ubermedia
for date in dates:
    day = "{:02d}".format(date.day)
    month = '{:02d}'.format(date.month)
    year = date.year

    data = spark.read.parquet(f"s3://near-data-warehouse/refined/dataPartner=ubermedia/year={year}/month={month}/day={day}/hour=*/country={country}/*")
    data = data.filter((col('src') == 'complementics') | (col('src') == 'irys') | (col('src') == 'predicio') | (col('src') == 'xmode'))
    data = data.drop('lat').drop('lng')
    data = data.withColumnRenamed('geoHash9', 'geohash')
    ff_df = data.join(poi, on='geohash', how='inner').drop('geohash')
    ff_df.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3992/poi_footfall/ubermedia/21_30/{}/".format(date))