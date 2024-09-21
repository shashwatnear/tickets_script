from pyspark.sql import *

spark = SparkSession.builder.appName("exclusive_ids").getOrCreate()

df1 = spark.read.csv('s3://staging-near-data-analytics/rule_engine_data/segment_id=89237_20240518/part-00000-c4fde3d7-c70f-4f16-927c-61c4109711c4-c000.csv')
df2 = spark.read.csv('s3://staging-near-data-analytics/rule_engine_data/segment_id=89684_20240518/part-00000-611b8bd8-8fc6-4fbb-9f26-ba3868d8cd34-c000.csv')
df3 = df1.exceptAll(df2)

df1 = spark.read.csv('s3://staging-near-data-analytics/rule_engine_data/segment_id=89237_20240518/part-00000-c4fde3d7-c70f-4f16-927c-61c4109711c4-c000.csv')
df2 = spark.read.csv('s3://staging-near-data-analytics/rule_engine_data/segment_id=89684_20240518/part-00000-611b8bd8-8fc6-4fbb-9f26-ba3868d8cd34-c000.csv')
df3 = df1.join(df2)
df3.coalesce(1).write.mode('append').csv(f"s3://staging-near-data-analytics/shashwat/matching/ids/")
