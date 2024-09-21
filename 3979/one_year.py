from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3979_one_year").getOrCreate()

def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 1609, 7)
    return a
generate_geohash_udf = udf(get_geohash)

start = "2024-05-28"
end = '2024-07-31'

dates = get_dates_between(start, end)

poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3979/files/3979.csv', header = True)
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))
poi.show()

rtb = ['gadx', 'indicue', 'inmobi', 'jorte', 'mfx', 'rubicon', 'xmode', 'yeahmobi']
sdk = ['complementics', 'irys', 'predicio', 'tamoco']

# for dp in rtb:
#     for date in dates:
#         try:
#             day = "{:02d}".format(date.day)
#             month = '{:02d}'.format(date.month)
#             year = date.year
#             df = spark.read.parquet(f's3://near-datamart/staypoints/version=v4/dataPartner={dp}/year={year}/month={month}/day={day}/hour=*/country=CAN/*')
#             df = df.filter(col('hdp_flag') == 0)
#             df = df.select(['ifa', df.geoHash9.substr(1,7).alias('geohash')])
#             df = df.withColumn('data_partner', lit(dp))
#             ff_df = df.join(poi, on='geohash', how='inner')
#             ff_df = ff_df.select('ifa', 'data_partner')
#             ff_df.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3979/one_year/stay/rtb/{dp}/{date}/")
#         except:
#             print(date, " Path does not exists")

# for dp in sdk:
#     for date in dates:
#         try:
#             day = "{:02d}".format(date.day)
#             month = '{:02d}'.format(date.month)
#             year = date.year
#             df = spark.read.parquet(f's3://near-datamart/staypoints/version=v6/dataPartner={dp}/year={year}/month={month}/day={day}/hour=*/country=CAN/*')
#             df = df.filter(col('hdp_flag') == 0)
#             df = df.select(['ifa', df.geoHash9.substr(1,7).alias('geohash')])
#             df = df.withColumn('data_partner', lit(dp))
#             ff_df = df.join(poi, on='geohash', how='inner')
#             ff_df = ff_df.select('ifa', 'data_partner')
#             ff_df.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3979/one_year/stay/sdk/{dp}/{date}/")
#         except:
#             print(date, " Path does not exists")

for date in dates:
    try:
        rule = spark.read.parquet(f's3://near-rule-engine-data/rule-input/CAN/{date}/*')
        rule = rule.filter(col('hdp_flag') == 0)
        rule = rule.select(['aspkId', rule.GH8.substr(1,7).alias('geohash')])
        rule = rule.join(poi, on='geohash', how='inner')
        rule = rule.select('aspkId')
        print("Date nowwwwwwwwwww", date)
        rule.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3979/one_year/rule_engine/{date}/")
    except:
        print(date, " Path does not exists")

# ###################