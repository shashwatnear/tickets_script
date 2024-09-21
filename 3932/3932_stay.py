from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3932_stay").getOrCreate()

def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 50, 8)
    return a
generate_geohash_udf = udf(get_geohash)

poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3932/files/SMRT-Dashboard location master v1 - Sheet1.csv', header = True)
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))
poi.show()

start = "2024-05-01"
end = "2024-05-31"
country = "SGP"

dates = get_dates_between(start, end)
hours = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"]

# Footfall report
for date in dates:
    for hour in hours:
        try:
            day = "{:02d}".format(date.day)
            month = '{:02d}'.format(date.month)
            year = date.year
            dest_path = "s3://staging-near-data-analytics/shashwat/ps-3932/staypoints/footfall/{}/{}".format(date, hour)

            data = spark.read.parquet(f"s3://near-datamart/staypoints/version=*/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country={country}/*")
            data = data.select(['ifa', data.geoHash9.substr(1, 8).alias('geohash'), 'coreSeg'])
            ff_df = data.join(poi, on='geohash', how='inner').drop('geohash')
            ff_df.write.mode("append").parquet(dest_path)
        except:
            print(date, " Path does not exists")



