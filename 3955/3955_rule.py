from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from util import *

spark = SparkSession.builder.appName("3955_rule").getOrCreate()

places_df = spark.read.csv("s3://staging-near-data-analytics/shashwat/ps-3955/files/Sensitive_FamilyPlanning_gh8.csv", header = True)

start = "2024-01-01"
end = "2024-07-11"
dates = get_dates_between(start, end)

for date in dates:
    data = spark.read.parquet(f"s3://near-rule-engine-data/rule-input/USA/{date}/*")
    data = data.select(['aspkId', data.GH8.alias('geohash')])
    ff_df = data.join(places_df, on='geohash', how='inner')
    ff_df = ff_df.select(['aspkId', 'geohash'])
    ff_df = ff_df.repartition(5)
    ff_df.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3955/rule/footfall/{date}/")