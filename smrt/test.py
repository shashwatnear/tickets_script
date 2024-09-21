from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash
import hashlib

spark = SparkSession.builder.appName("smrt_test").getOrCreate()

start = "2024-07-01"
end = "2024-07-31"
country = "SGP"

dates = get_dates_between(start, end)

# UDFs
def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 50, 8)
    return a
generate_geohash_udf = udf(get_geohash)

poi = spark.read.csv('s3://near-data-analytics/shashwat/bayer/sample_smrt.csv', header = True)
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))
poi.show()

rtb = ['gadx', 'indicue', 'inmobi', 'jorte', 'mfx', 'rubicon', 'xmode', 'yeahmobi']
sdk = ['complementics', 'irys', 'predicio', 'tamoco']

for dp in rtb:
    for date in dates:
        try:
            day = "{:02d}".format(date.day)
            df = spark.read.parquet(f's3://near-datamart/staypoints/version=v4/dataPartner={dp}/year=2024/month=07/day={day}/hour=*/country=SGP/*')
            df = df.filter(col('hdp_flag') == 0)
            df = df.select(['ifa', df.geoHash9.substr(1,8).alias('geohash')])
            df = df.withColumn('data_partner', lit(dp))
            ff_df = df.join(poi, on='geohash', how='inner')
            ff_df = ff_df.select('ifa', 'data_partner')
            ff_df.write.mode("overwrite").parquet(f"s3://staging-near-data-analytics/shashwat/smrt/test/rtb/july/{dp}/{date}/")
        except:
            print(date, " Path does not exists")

for dp in sdk:
    for date in dates:
        try:
            day = "{:02d}".format(date.day)
            df = spark.read.parquet(f's3://near-datamart/staypoints/version=v6/dataPartner={dp}/year=2024/month=07/day={day}/hour=*/country=SGP/*')
            df = df.filter(col('hdp_flag') == 0)
            df = df.select(['ifa', df.geoHash9.substr(1,8).alias('geohash')])
            df = df.withColumn('data_partner', lit(dp))
            ff_df = df.join(poi, on='geohash', how='inner')
            ff_df = ff_df.select('ifa', 'data_partner')
            ff_df.write.mode("overwrite").parquet(f"s3://staging-near-data-analytics/shashwat/smrt/test/sdk/july/{dp}/{date}/")
        except:
            print(date, " Path does not exists")