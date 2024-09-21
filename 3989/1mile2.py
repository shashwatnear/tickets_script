from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from util import *
from pyspark.sql.functions import row_number
from pyspark.sql.window import *
import proximityhash


spark = SparkSession.builder.appName("3989_1mile").getOrCreate()

def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 1609, 7)
    return a
generate_geohash_udf = udf(get_geohash)

poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3989/files/poi.csv', header = True)
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))
poi.show()

start = "2024-04-05"
end = "2024-06-04"
dates = get_dates_between(start, end)

for date in dates:
    try:
        day = "{:02d}".format(date.day)
        month = '{:02d}'.format(date.month)
        year = date.year

        data = spark.read.parquet(f"s3://near-datamart/staypoints/version=*/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country=USA/*")
        data = data.where(col('hdp_flag') == 0)
        data = data.select(['aspkId', data.geoHash9.substr(1, 7).alias('geohash')])
        ff_df = data.join(poi, on='geohash', how='inner')
        ff_df = ff_df.select('aspkId')
        ff_df = ff_df.repartition(4)
        ff_df.write.mode("overwrite").parquet("s3://staging-near-data-analytics/shashwat/ps-3989/stay/1mile/footfall_gh7/{}/".format(date))
    except:
        print(date, " Path does not exists")

# ################################################

# stay = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3989/1mile/footfall_gh7/*/*')

# seg_id = [91279, 91280, 91281, 91283, 91284]

# for id in seg_id:
#     try:
#         df = spark.read.parquet(f"s3://near-rule-engine-data/rule-output/segment={id}/date=*/*")
#         df = df.select(["aspkId"])
#         total = df.join(stay, on='aspkId', how = 'inner')
#         df.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3989/matching/1Mile/segment_id={id}/")
#         print(f"Completed for seg_id: {id}")
#     except Exception as e:
#         print(e)

# ###############################################

# seg_id = [91279, 91280, 91281, 91283, 91284]

# for id in seg_id:
#     df = spark.read.parquet(f"s3://staging-near-data-analytics/shashwat/ps-3989/matching/1Mile/segment_id={id}/*")
#     df.agg(countDistinct('aspkId')).show()
