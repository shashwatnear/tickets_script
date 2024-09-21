from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3979").getOrCreate()

def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 1609, 7)
    return a
generate_geohash_udf = udf(get_geohash)

start = "2024-07-25"
end = '2024-07-31'

dates = get_dates_between(start, end)

poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3979/files/3979.csv', header = True)
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
            df = spark.read.parquet(f's3://near-datamart/staypoints/version=*/dataPartner={dp}/year=2024/month=07/day={day}/hour=*/country=CAN/*')
            df = df.filter(col('hdp_flag') == 0)
            df = df.select(['ifa', df.geoHash9.substr(1,7).alias('geohash')])
            df = df.withColumn('data_partner', lit(dp))
            ff_df = df.join(poi, on='geohash', how='inner')
            ff_df.write.mode("overwrite").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3979/one_week_sample/rtb/{dp}/{date}/")
        except:
            print(date, " Path does not exists")

for dp in sdk:
    for date in dates:
        try:
            day = "{:02d}".format(date.day)
            df = spark.read.parquet(f's3://near-datamart/staypoints/version=v6/dataPartner={dp}/year=2024/month=07/day={day}/hour=*/country=CAN/*')
            df = df.filter(col('hdp_flag') == 0)
            df = df.select(['ifa', df.geoHash9.substr(1,7).alias('geohash')])
            df = df.withColumn('data_partner', lit(dp))
            ff_df = df.join(poi, on='geohash', how='inner')
            ff_df.write.mode("overwrite").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3979/one_week_sample/sdk/{dp}/{date}/")
        except:
            print(date, " Path does not exists")

# ######################

df1 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3979/one_week_sample/rtb/*/*/*')
df2 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3979/one_week_sample/sdk/*/*/*')
df1.groupBy('data_partner').agg(countDistinct('ifa').alias('ifa_cnt')).show()
df2.groupBy('data_partner').agg(countDistinct('ifa').alias('ifa_cnt')).show()