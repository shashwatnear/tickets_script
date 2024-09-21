from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import collect_list, concat_ws
import datetime
from util import *
import hashlib

spark = SparkSession.builder.appName("3928").getOrCreate()

def computeHash(ID):
    m = hashlib.sha1((ID.upper() + "9o8WnUtwdY").encode())
    return m.hexdigest()

sha_udf = udf(computeHash,StringType())

def repl(seg):
    return seg.replace(', ', '      ')
replace_seg = udf(repl)

rep = udf(replace_seg)  

# start = "2024-02-15"
# end = "2024-05-15"
# dates = get_dates_between(start, end)

seg_id = ["89758", "89759", "89760", "89761", "89763"]

# for id in seg_id:
#     try:
#         for date in dates:    
#             try:
#                 source_path = f"s3://near-rule-engine-data/rule-output/segment={id}/date={date}/*"
#                 write_path = f"s3://staging-near-data-analytics/shashwat/ps-3928/unhashed_ids_temp/segment_id={id}/{date}/"
#                 print(source_path)
#                 df = spark.read.parquet(source_path)
#                 df = df.select("aspkId").distinct()
#                 df.write.mode("overwrite").parquet(write_path)
#             except:
#                 print(f"Path Doesn't Exist for date: {date}")
#         print(f"Completed for seg_id: {id}")
#     except Exception as e:
#         print(e)

# ################################
# ifa_df = spark.read.parquet('s3://near-data-warehouse/id-manager-offline/current/*')
# ifa_df = ifa_df.select('ifa','aspk_id')
# ifa_df = ifa_df.withColumn('ifa', upper(col('ifa')))
# ifa_df = ifa_df.withColumnRenamed('aspk_id',"aspkId")

# for id in seg_id:
#     df = spark.read.parquet(f's3://staging-near-data-analytics/shashwat/ps-3928/unhashed_ids_temp/segment_id={id}/*')
#     df = ifa_df.join(df, on='aspkId', how='inner')
#     df = df.select('ifa').drop_duplicates()
#     df.coalesce(1).write.mode("overwrite").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3928/unhashed_ids/segment_id={id}/")

# ################################

poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3928/Lumos_Azira_SoW - Segment Details.csv', header = True)
dataframes = []
for id in seg_id:
    df = spark.read.parquet(f's3://staging-near-data-analytics/shashwat/ps-3928/unhashed_ids/segment_id={id}/*')
    df = df.withColumn('maid', sha_udf(col('ifa'))).drop('ifa')
    df = df.withColumn('segment_id', lit(id))
    df = df.join(poi, on='segment_id', how='inner')
    dataframes.append(df)
    grouped_df = df.groupBy("maid").agg(collect_list("name").alias("names")).withColumn('names', concat_ws(', ', col('names')))
    result_df = grouped_df.withColumn('ifa_seg', concat(col('maid'),col('names'))).withColumn('ifa_seg', rep(col('ifa_seg')))

# # Union all DataFrames in the list
# final_df = dataframes[0]
# for df in dataframes[1:]:
#     final_df = final_df.union(df)

# # Write the final DataFrame to a single CSV file with tab separator
# final_df.coalesce(1).write.option("delimiter", "\t").csv("s3://staging-near-data-analytics/shashwat/ps-3928/raw_report/")





# ###########

# df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3928/maid_namesList/')
# # df1 = df.groupBy('maid').agg(collect_list('names').alias('names'))
# df1 = df.withColumn('names', concat_ws(', ', 'names'))

# # df1 = df1.withColumn('names', concat('maid', 'names')).withColumn('final', replace_seg('names'))
# df1 = df1.withColumn('names', concat('maid',lit('   ') ,'names')).withColumn('final', replace_seg('names'))
# df1.write.mode("overwrite").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3928/final_new/")

# #############

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3928/final_new/')
df = df.select('final')
df.coalesce(1).write.option("delimiter", "\t").format('csv').save("s3://staging-near-data-analytics/shashwat/ps-3928/finalreporttsv1/Azira_MI_POC_Segments_MYS.tsv")
spark.stop()

# 8-4-4-4-12 unhashed device id | ifa

# final = spark.read.csv('s3://staging-near-data-analytics/Chandrashekhar/postcode_final/cardId_segmentId_fpid/*/*')


