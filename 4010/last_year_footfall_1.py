from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from util import *

spark = SparkSession.builder.appName("Footfall on POI").getOrCreate()

start = '2023-08-01'
end = '2023-08-30'

dates = get_dates_between(start, end)

poi = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4010/campaign_data/places_geohash/Oregon/*')

for date in dates:
    try:
        day = "{:02d}".format(date.day)
        month = '{:02d}'.format(date.month)
        year = date.year

        data = spark.read.parquet(f"s3://near-datamart/staypoints/version=*/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country=USA/*")
        data = data.where(col('hdp_flag') == 0)
        data = data.select(['ifa', data.geoHash9.alias('geohash')])
        ff_df = data.join(poi, on='geohash', how='inner')
        ff_df = ff_df.select(['ifa', 'near_poi_id'])
        ff_df = ff_df.repartition(4)
        ff_df.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-4010/last_year_footfall_gh9_stay/{}/".format(date))
    except:
        print(date, " Path does not exists")