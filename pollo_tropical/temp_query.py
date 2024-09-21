from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, avg, collect_list, udf
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("geohash_to_lat_lon").getOrCreate()

def calculate_median(values_list):
    sorted_list = sorted(values_list)
    length = len(sorted_list)
    if length % 2 == 0:
        return float((sorted_list[length // 2 - 1] + sorted_list[length // 2]) / 2.0)
    else:
        return float(sorted_list[length // 2])
median_udf = udf(calculate_median, FloatType())

df = spark.read.csv('s3://staging-near-data-analytics/shashwat/acm/ifa_distance/*', header = True)
df = df.withColumn('distance_meters', col('distance_meters').cast('float'))
# poi_distinctIds = df.groupBy('near_poi_id').agg(countDistinct('ifa'))
# poi_distinctIds.coalesce(1).write.mode("append").csv(f"s3://staging-near-data-analytics/shashwat/acm/nikhil_temp_query/poi_wise_ids/", header = True)

avg_df = df.groupBy('near_poi_id').agg(avg('distance_meters').alias('average'))

median_df = df.groupBy("near_poi_id").agg(collect_list("distance_meters").alias("values_list")) \
               .withColumn("median", median_udf(col("values_list"))) \
               .select("near_poi_id", "median")
result_df = avg_df.join(median_df, on="near_poi_id")
result_df.coalesce(1).write.mode("append").csv(f"s3://staging-near-data-analytics/shashwat/acm/nikhil_temp_query/poi_wise_avg_median/", header = True)

# df.agg(countDistinct('ifa')).show()
# df.agg(avg('distance_meters')).show()

# df = df.withColumn("value", col("distance_meters").cast(FloatType()))
# values_list = df.select("value").rdd.flatMap(lambda x: x).collect()
# values_list.sort()
# length = len(values_list)
# if length % 2 == 0:
#     overall_median = (values_list[length // 2 - 1] + values_list[length // 2]) / 2.0
# else:
#     overall_median = values_list[length // 2]

# print(f"Overall Median: {overall_median}")