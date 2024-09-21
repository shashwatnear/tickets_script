from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from util import *

spark = SparkSession.builder.appName("3955_rule").getOrCreate()

places_df = spark.read.csv("s3://staging-near-data-analytics/shashwat/ps-3955/files/Sensitive_FamilyPlanning_gh9.csv", header = True)

start = "2024-05-01"
end = "2024-05-31"
dates = get_dates_between(start, end)

for date in dates:
    day = "{:02d}".format(date.day)
    month = '{:02d}'.format(date.month)
    year = date.year

    data = spark.read.parquet(f"s3://near-datamart/staypoints/version=*/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country=USA/*")
    data = data.where(col('hdp_flag') == 0)
    data = data.select(['aspkId', data.geoHash9.alias('geohash')])
    ff_df = data.join(places_df, on='geohash', how='inner')
    ff_df = ff_df.select(['aspkId', 'geohash'])
    ff_df = ff_df.repartition(5)
    ff_df.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3955/stay_may/footfall/{date}/")

for date in dates:
    try:
        ref = spark.read.parquet(f's3://staging-near-data-analytics/shashwat/ps-3955/stay_may/footfall/{date}/*')
        ref = ref.withColumn('date', lit(date))
        ref = ref.groupBy('date').agg(countDistinct('aspkId').alias('ids'))
        ref.write.mode('append').csv(f's3://staging-near-data-analytics/shashwat/ps-3955/intermediate/stay_may/{date}/', header=True)
    except Exception as e:
        print(f"Error processing date {date}: {e}")

# Now read all intermediate files and combine them
intermediate_files = 's3://staging-near-data-analytics/shashwat/ps-3955/intermediate/stay_may/*'
result = spark.read.csv(intermediate_files, header=True)
result = result.orderBy('date')

result.coalesce(1).write.mode('append').csv(f's3://staging-near-data-analytics/shashwat/ps-3955/final/stay_may/', header = True)

spark.stop()