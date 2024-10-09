from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from util import *
import hashlib

spark = SparkSession.builder.appName("4019").getOrCreate()

# def computeHash(ID):
#     try:
#         m = hashlib.sha256(ID.upper().encode('utf-8'))
#         return m.hexdigest()
#     except:
#         print('No IP for this ip ', ID)
# sha_udf = udf(computeHash)

'''
+------------------------------------+----------------------------------------------------------------+-----------------------+
|_c0                                 |_c1                                                             |_c2                    |
+------------------------------------+----------------------------------------------------------------+-----------------------+
|1bdc258f-3029-bbe2-1d99-32f05769fa7f|f6e7122e2c7636940dd7929ee8beacfef84c2fb16a15887378ad0c0d99187a75|2024-09-16 00:38:16.000|
|3053f750-8e47-b6a9-e032-2a29498ce349|3759074d66f56373666f51623fcb6ac836fb58f528c988685a51199c79b833fa|2024-09-06 04:02:27.000|
|3053f750-8e47-b6a9-e032-2a29498ce349|13cd2d55f9333df8006af2f0009fcd6ed1d03dcc636a26f3176a13e4ee1e763a|2024-08-24 23:27:00.000|
|3053f750-8e47-b6a9-e032-2a29498ce349|15d64d287336874c949488a624be53042dd59d9e160dc7dcdd02d3cacba38be9|2024-09-09 06:33:57.000|
|3053f750-8e47-b6a9-e032-2a29498ce349|15d64d287336874c949488a624be53042dd59d9e160dc7dcdd02d3cacba38be9|2024-09-15 12:23:43.000|
+------------------------------------+----------------------------------------------------------------+-----------------------+
'''

# start = "2024-08-18"
# end = "2024-09-17"
# dates = get_dates_between(start, end)

# country = "SGP"

# for date in dates:
#     day = "{:02d}".format(date.day)
#     month = '{:02d}'.format(date.month)
#     year = date.year 

#     refined = spark.read.parquet(f"s3://near-data-warehouse/refined/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country={country}/*").select(['ifa', 'devIp', 'eventDTLocal'])
#     refined = refined.withColumn('hashed_ip', sha_udf(col('devIp')))
#     refined.write.parquet(f"s3://staging-near-data-analytics/shashwat/ps-4019/refined/{date}/")

# ######################### Overall match ########################

# refined = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4019/refined/*/*')
# # 1442994

# df = spark.read.csv('s3://near-data-analytics/nikhil/azira_3rd_party_data_vendor_sg.csv.gz')
# df = df.withColumnRenamed('_c0', 'tifa').withColumnRenamed('_c1', 'device_ipv4').withColumnRenamed('_c2', 'event_time')
# # 374142

# final = refined.join(df, refined.hashed_ip == df.device_ipv4)
# 227837
# final.write.parquet(f"s3://staging-near-data-analytics/shashwat/ps-4019/refined_samsung_join/")

# ######################### Date wise match ########################

# refined = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4019/refined/*/*')
# refined = refined.withColumn('date', col('eventDTLocal').substr(1, 10))

# df = spark.read.csv('s3://near-data-analytics/nikhil/azira_3rd_party_data_vendor_sg.csv.gz')
# df = df.withColumnRenamed('_c0', 'tifa').withColumnRenamed('_c1', 'hashed_ip').withColumnRenamed('_c2', 'event_time')
# df = df.withColumn('date', col('event_time').substr(1, 10))

# final = refined.join(df, (refined['date'] == df['date']) & (refined['hashed_ip'] == df['device_ipv4']), 'inner').drop(df['date'])
# final = final.groupBy('date').agg(countDistinct('hashed_ip').alias('common_ips'))
# final.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4019/daywise_match/")


# ######################## Date wise distinct IPs #########################

# from pyspark.sql.functions import col, countDistinct

# # Step 1: Read the refined data
# refined = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4019/refined/*/*')
# refined = refined.withColumn('date', col('eventDTLocal').substr(1, 10))

# # Step 2: Read the CSV and rename columns
# df = spark.read.csv('s3://near-data-analytics/nikhil/azira_3rd_party_data_vendor_sg.csv.gz')
# df = df.withColumnRenamed('_c0', 'tifa') \
#        .withColumnRenamed('_c1', 'hashed_ip') \
#        .withColumnRenamed('_c2', 'event_time') \
#        .withColumn('date', col('event_time').substr(1, 10))

# # Step 3: Find common IPs between the two DataFrames on both 'hashed_ip' and 'date'
# common_ips_df = refined.join(df, on=['hashed_ip', 'date'], how='inner').select('date', 'hashed_ip')

# # Step 4: Count occurrences of each (hashed_ip, date) pair
# ip_day_counts_df = common_ips_df.groupBy("hashed_ip", "date").count()

# # Step 5: Filter to get only IPs that appear on exactly one day (i.e., unique per day)
# unique_ips_df = ip_day_counts_df.filter("count == 1").select("hashed_ip", "date")

# # Step 6: Group by date to count the distinct IPs that were unique on that day
# distinct_day_wise_count_df = unique_ips_df.groupBy("date").agg(countDistinct("hashed_ip").alias("distinct_ip_count"))

# # Step 7: Write the result to S3 as a CSV
# distinct_day_wise_count_df.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4019/daywise_unique_matched_ids/")


# ##################### total unique day wise #######################

# from pyspark.sql.functions import col, countDistinct

# # Step 1: Read the refined data
# refined = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4019/refined/*/*')
# refined = refined.withColumn('date', col('eventDTLocal').substr(1, 10))

# # Step 2: Read the CSV and rename columns
# df = spark.read.csv('s3://near-data-analytics/nikhil/azira_3rd_party_data_vendor_sg.csv.gz')
# df = df.withColumnRenamed('_c0', 'tifa') \
#        .withColumnRenamed('_c1', 'hashed_ip') \
#        .withColumnRenamed('_c2', 'event_time') \
#        .withColumn('date', col('event_time').substr(1, 10))

# # Step 3: Find common IPs between the two DataFrames on both 'hashed_ip' and 'date'
# common_ips_df = refined.join(df, on=['hashed_ip', 'date'], how='inner').select('date', 'hashed_ip')

# # Step 4: Count occurrences of each (hashed_ip, date) pair
# ip_day_counts_df = common_ips_df.groupBy("hashed_ip", "date").count()

# # Step 5: Filter to get only IPs that appear on exactly one day (i.e., unique per day)
# unique_ips_df = ip_day_counts_df.filter("count == 1").select("hashed_ip", "date")

# # Step 6: Get distinct IPs for each day
# distinct_day_wise_ips_df = unique_ips_df.groupBy("date").agg(countDistinct("hashed_ip").alias("distinct_ip_count"))

# # Step 7: Union all the unique IPs from each day to get the complete set of unique IPs across the entire period
# all_unique_ips_df = unique_ips_df.select("hashed_ip").distinct()

# # Step 8: Count the total number of distinct IPs across all days
# total_unique_ip_count = all_unique_ips_df.count()

# # Step 9: Write the total unique IPs to S3 (if you need to save it)
# all_unique_ips_df.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4019/total_unique_ips_across_days/")

# ############### #############

# from pyspark.sql.functions import col, countDistinct, weekofyear

# # Step 1: Read the refined data
# refined = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4019/refined/*/*')
# refined = refined.withColumn('date', col('eventDTLocal').substr(1, 10))

# # Step 2: Read the CSV and rename columns
# df = spark.read.csv('s3://near-data-analytics/nikhil/azira_3rd_party_data_vendor_sg.csv.gz')
# df = df.withColumnRenamed('_c0', 'tifa') \
#        .withColumnRenamed('_c1', 'hashed_ip') \
#        .withColumnRenamed('_c2', 'event_time') \
#        .withColumn('date', col('event_time').substr(1, 10))

# # Step 3: Match both DataFrames based on 'hashed_ip' and 'date'
# common_ips_df = refined.join(df, on=['hashed_ip', 'date'], how='inner').select('date', 'hashed_ip')

# # Step 2: Drop duplicates based on 'hashed_ip' to get distinct IPs across all days
# distinct_ips_df = common_ips_df.dropDuplicates(['hashed_ip'])

# # Step 3: Count the total number of distinct IPs across all days
# total_distinct_ips_count = distinct_ips_df.select('hashed_ip').distinct().count()

# # Output the result
# print(f"Total number of distinct IPs matched across all days: {total_distinct_ips_count}")

# # Optionally: Save the distinct IPs to S3
# distinct_ips_df.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4019/total_distinct_ips_across_days/")

# ############### #############

# # Total distinct IPs across weeks
# from pyspark.sql.functions import weekofyear

# # Step 1: Read the refined data
# refined = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4019/refined/*/*')
# refined = refined.withColumn('date', col('eventDTLocal').substr(1, 10))

# # Step 2: Read the CSV and rename columns
# df = spark.read.csv('s3://near-data-analytics/nikhil/azira_3rd_party_data_vendor_sg.csv.gz')
# df = df.withColumnRenamed('_c0', 'tifa') \
#        .withColumnRenamed('_c1', 'hashed_ip') \
#        .withColumnRenamed('_c2', 'event_time') \
#        .withColumn('date', col('event_time').substr(1, 10))

# # Step 3: Match both DataFrames based on 'hashed_ip' and 'date'
# common_ips_df = refined.join(df, on=['hashed_ip', 'date'], how='inner').select('date', 'hashed_ip').dropDuplicates()
# days = common_ips_df.select('date', 'hashed_ip').dropDuplicates()
# days = days.orderBy('date')
# days.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4019/total_distinct_ips_across_days/")

# weeks = common_ips_df.withColumn('week', weekofyear(col('date')))
# weeks = weeks.select('week', 'hashed_ip').dropDuplicates()
# weeks = weeks.orderBy('week')
# weeks.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4019/total_distinct_ips_across_weeks/")

# # #################
# from pyspark.sql import functions as F
# # Total distinct IPs across weeks
# from pyspark.sql.functions import weekofyear

# # Step 1: Read the refined data
# refined = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4019/refined/*/*')
# refined = refined.withColumn('date', col('eventDTLocal').substr(1, 10))

# # Step 2: Read the CSV and rename columns
# df = spark.read.csv('s3://near-data-analytics/nikhil/azira_3rd_party_data_vendor_sg.csv.gz')
# df = df.withColumnRenamed('_c0', 'tifa') \
#        .withColumnRenamed('_c1', 'hashed_ip') \
#        .withColumnRenamed('_c2', 'event_time') \
#        .withColumn('date', col('event_time').substr(1, 10))

# # Step 3: Match both DataFrames based on 'hashed_ip' and 'date'
# common_ips_df = refined.join(df, on=['hashed_ip', 'date'], how='inner').select('date', 'hashed_ip').dropDuplicates()

# # By doing drop duplicates, we will have unique IDs for each day then we do count distinct so we'll get overall unique IDs
# days = common_ips_df.select('date', 'hashed_ip').dropDuplicates()
# days.agg(countDistinct('hashed_ip')).show()

# # By doing drop duplicates, we will have unique IDs for each week then we do count distinct so we'll get overall unique IDs
# weeks = common_ips_df.withColumn('week', weekofyear(col('date')))
# weeks = weeks.select('week', 'hashed_ip').dropDuplicates()
# weeks.agg(countDistinct('hashed_ip')).show()

# +- 3 days over a week

from pyspark.sql import functions as F

# Step 1: Read the refined data
df1 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4019/refined/*/*')
df1 = df1.withColumn('date', col('eventDTLocal').substr(1, 10))

# Step 2: Read the CSV and rename columns
df2 = spark.read.csv('s3://near-data-analytics/nikhil/azira_3rd_party_data_vendor_sg.csv.gz')
df2 = df2.withColumnRenamed('_c0', 'tifa') \
       .withColumnRenamed('_c1', 'hashed_ip') \
       .withColumnRenamed('_c2', 'event_time') \
       .withColumn('date', col('event_time').substr(1, 10))

# Assuming df1 and df2 both have 'hashed_ip' and 'date' columns
# Convert the 'date' columns to DateType if they are not already
df1 = df1.withColumn('date', F.to_date('date'))
df2 = df2.withColumn('date', F.to_date('date'))

# Rename the date columns to avoid ambiguity after the join
df1 = df1.withColumnRenamed('date', 'df1_date')
df2 = df2.withColumnRenamed('date', 'df2_date')

# Perform a join on 'hashed_ip'
joined_df = df1.join(df2, on='hashed_ip', how='inner')

# Apply the date range filter (+/- 7 days)
result_df = joined_df.filter(
    (F.datediff(joined_df['df1_date'], joined_df['df2_date']) <= 3) &
    (F.datediff(joined_df['df1_date'], joined_df['df2_date']) >= -3)
)

# Select relevant columns
result_df = result_df.select('hashed_ip', 'df1_date', 'df2_date').dropDuplicates()
result_df.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4019/3_days/")



