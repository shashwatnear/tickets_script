from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from util import *
from pyspark.sql.functions import row_number
from pyspark.sql.window import *
import proximityhash


spark = SparkSession.builder.appName("3989_3_mile").getOrCreate()

def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 4828, 7)
    return a
generate_geohash_udf = udf(get_geohash)

poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3989/files/poi.csv', header = True)
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))
poi.show()

start = "2024-06-05"
end = "2024-08-05"
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
        ff_df.write.mode("overwrite").parquet("s3://staging-near-data-analytics/shashwat/ps-3989/stay/3mile/footfall_gh7/{}/".format(date))
    except:
        print(date, " Path does not exists")

# ################################################

# seg_id = [91279, 91280, 91281, 91283, 91284]

# for id in seg_id:
#     try:
#         for date in dates:    
#             try:
#                 source_path = f"s3://near-rule-engine-data/rule-output/segment={id}/date={date}/*"
#                 print(source_path)
#                 df = spark.read.parquet(source_path)
#                 df = df.select(["aspkId", df.geoHash8.substr(1,7).alias("geohash")])
#                 df = df.join(poi, on='geohash', how = 'inner').select('aspkId')
#                 df.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3989/rule/3Mile/segment_id={id}/{date}/")
#             except:
#                 print(f"Path Doesn't Exist for date: {date}")
#         print(f"Completed for seg_id: {id}")
#     except Exception as e:
#         print(e)
        
# ################################################