from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from util import *

spark = SparkSession.builder.appName("3955_rule").getOrCreate()

places_df = spark.read.csv("s3://staging-near-data-analytics/shashwat/ps-3955/files/Sensitive_FamilyPlanning_single_polygon_gh8.csv", header = True)

start = "2024-07-01"
end = "2024-07-07"
dates = get_dates_between(start, end)

for date in dates:
    data = spark.read.parquet(f"s3://near-rule-engine-data/rule-input/USA/{date}/*")
    data = data.select(['aspkId', data.GH8.alias('geohash')])
    ff_df = data.join(places_df, on='geohash', how='inner')
    ff_df = ff_df.select(['aspkId', 'geohash', 'lat', 'lon'])
    ff_df = ff_df.repartition(5)
    ff_df.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3955/3_days_sample_rule/footfall/{date}/")

# #######################
    
# for date in dates:
#     try:
#         ref = spark.read.parquet(f's3://staging-near-data-analytics/shashwat/ps-3955/3_days_sample_rule/footfall/{date}/*')
#         ref = ref.withColumn('date', lit(date))
#         ref = ref.groupBy('date').agg(countDistinct('aspkId').alias('ids'))
#         ref.write.mode('overwrite').csv(f's3://staging-near-data-analytics/shashwat/ps-3955/intermediate/3_days_sample_rule/{date}/', header=True)
#     except Exception as e:
#         print(f"Error processing date {date}: {e}")

# # Now read all intermediate files and combine them
# intermediate_files = f's3://staging-near-data-analytics/shashwat/ps-3955/intermediate/3_days_sample_rule/*'
# result = spark.read.csv(intermediate_files, header=True)

# result.coalesce(1).write.mode('append').csv(f's3://staging-near-data-analytics/shashwat/ps-3955/final/3_days_sample_rule/', header=True)