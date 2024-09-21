from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from util import *
from pyspark.sql.functions import row_number
from pyspark.sql.window import *
import proximityhash


spark = SparkSession.builder.appName("3989_1mile").getOrCreate()

# def get_geohash(lat, lon):
#     a = proximityhash.create_geohash(float(lat), float(lon), 1609, 7)
#     return a
# generate_geohash_udf = udf(get_geohash)

# poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3989/files/poi.csv', header = True)
# poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
# poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
# poi = poi.withColumn('geohash',explode('geohash'))
# poi.show()

# start = "2024-02-09"
# end = "2024-08-05"
# dates = get_dates_between(start, end)

# for date in dates:
#     try:
#         day = "{:02d}".format(date.day)
#         month = '{:02d}'.format(date.month)
#         year = date.year

#         data = spark.read.parquet(f"s3://near-datamart/staypoints/version=*/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country=USA/*")
#         data = data.where(col('hdp_flag') == 0)
#         data = data.select(['aspkId', data.geoHash9.substr(1, 7).alias('geohash')])
#         ff_df = data.join(poi, on='geohash', how='inner')
#         ff_df = ff_df.select('aspkId')
#         ff_df = ff_df.repartition(4)
#         ff_df.write.mode("overwrite").parquet("s3://staging-near-data-analytics/shashwat/ps-3989/stay/1mile/footfall_gh7/{}/".format(date))
#     except:
#         print(date, " Path does not exists")

# ################################################

# GSA - Financial Planning - 33b5530ab9e82d6588bf48887d04b6b9 - 91355 - 267,401
# GSA - Head of household - 70986145dc59c2f147ca7457e5732d8c - 91354 - 108,932
# GSA - Credit/Debt & Loans - 649896aeb68323b721e9553cc051a10c - 91353 - 135,042
# GSA - Beginning Investing - b85ccbe098747f1e0660f8fcf519d3e3 - 91352 - 121,101
# GSA - Personal Finance - 07b5f0b5a2d73b2e22c2be83f373ec95 - 91351 - 106,605

stay = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3989/stay/3mile/footfall_gh7/*/*')

seg_id = [91355, 91354, 91353, 91352, 91351]

for id in seg_id:
    try:
        print(id)
        df = spark.read.parquet(f"s3://near-rule-engine-data/rule-output/segment={id}/date=*/*")
        df = df.select(["aspkId"])
        total = df.join(stay, on='aspkId', how = 'inner')
        total.agg(countDistinct('aspkId')).show()
        # df.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3989/matching/1Mile/segment_id={id}/")
    except Exception as e:
        print(e)

# ###############################################
