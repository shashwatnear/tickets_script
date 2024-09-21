from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from util import *
import proximityhash

spark = SparkSession.builder.appName("3935_h&m_dooh").getOrCreate()

def get_geohash(lat, lon):
    radius = 500
    geohash = 8
    a = proximityhash.create_geohash(float(lat), float(lon), radius, geohash)
    return a
generate_geohash_udf = udf(get_geohash)

dooh = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3935/files/h&m_dooh.csv', header = True)
dooh = dooh.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
dooh = dooh.withColumn('geohash', split(dooh['geohash'], ','))
dooh = dooh.withColumn('geohash', explode(dooh['geohash']))

start = "2024-05-17"
end = "2024-06-03"
country = "IND"

dates = get_dates_between(start, end)

for date in dates:
    day = "{:02d}".format(date.day)
    month = '{:02d}'.format(date.month)
    year = date.year
    dest_path = "s3://staging-near-data-analytics/shashwat/ps-3935/dooh_footfall/h&m/{}/".format(date)

    data = spark.read.parquet(f"s3://near-datamart/staypoints/version=*/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country={country}/*")
    data = data.where(col("hdp_flag") == 0)
    data = data.select(['aspkId', data.eventTs.substr(1,10).alias('loc_time'), data.geoHash9.substr(1, 8).alias('geohash')])
    ff_df = data.join(dooh, on='geohash', how='inner')
    ff_df.write.mode("append").parquet(dest_path)