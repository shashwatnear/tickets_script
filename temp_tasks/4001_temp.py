from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from util import *

spark = SparkSession.builder.appName("4001_analysis").getOrCreate()

df_imp = spark.read.parquet("s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=661fe05ffad7b653ba8be138/data_cache/impressions_backup/")

df_vis = spark.read.parquet("s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=661fe05ffad7b653ba8be138/data_cache/visits/location=b8b8a7e4b6f06f6a7ac36c6f3c136a6a/")

df_mat = df_imp.join(df_vis, (df_imp.ifa == df_vis.ifa) & (datediff(df_vis.date, df_imp.date).between(0,90)), "inner").drop(df_imp.ifa).drop(df_imp.date)

df_ip_ifa = df_mat.select(['ip', 'ifa']).dropDuplicates()

df_ip_ifa.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4001/campaign/dct/ip_ifa/")

# ###############

df_imp1 = spark.read.parquet("s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=65e22c0e29ba7472dcf64eb2/data_cache/impressions/")

df_vis = spark.read.parquet("s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=65e22c0e29ba7472dcf64eb2/data_cache/visits/location=08dbae1da23b4897e8595640eceac6eb/")

df_mat = df_imp1.join(df_vis, (df_imp1.ifa == df_vis.ifa) & (datediff(df_vis.date, df_imp1.date).between(0,30)), "inner").drop(df_imp1.ifa).drop(df_imp1.date)

df_ip_ifa = df_mat.select(['ip', 'ifa']).dropDuplicates()

df_ip_ifa.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4001/campaign/usa/ip_ifa/")