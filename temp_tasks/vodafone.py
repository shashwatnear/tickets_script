from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Window
import pyspark.sql.functions as f
from util import *
import proximityhash

spark = SparkSession.builder.appName("Vodafone").getOrCreate()

def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 1609, 8)
    return a
generate_geohash_udf = udf(get_geohash)

schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True)
])

data = [(41.067333, 29.013417)]
poi = spark.createDataFrame(data, schema)
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))

start = "2024-05-16"
end = "2024-08-16"

dates = get_dates_between(start, end)

for date in dates:
    day = "{:02d}".format(date.day)
    month = '{:02d}'.format(date.month)
    year = date.year
    df = spark.read.parquet(f's3://near-data-warehouse/refined/dataPartner=*/year=2024/month={month}/day={day}/hour=*/country=TUR/*')
    df = df.select(['aspkId', df.geoHash9.substr(1, 8).alias('geohash')])
    df = df.dropDuplicates()
    df = df.join(poi, on='geohash', how='inner').drop('geohash')
    df = df.select('aspkId')
    df.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/temp_tasks/vodafone/poi_footfall/refined/{}/".format(date))

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/temp_tasks/vodafone/poi_footfall/refined/*/*')
df.agg(countDistinct('aspkId')).show()