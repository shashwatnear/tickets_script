from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from util import *
from pyspark.sql.functions import create_map, lit, col, lower
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
import json

spark = SparkSession.builder.appName("4001_analysis").getOrCreate()

# campaign = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-4001/campaign/dct/ip_ifa/', header = True)

# # Load the data
# de = spark.read.option("header", "true").option("delimiter", ";").csv('s3://staging-near-data-analytics/shashwat/ps-4001/de_data/azira_results_2.csv', header=True)
# de = de.withColumn("host-info", regexp_replace(col("host-info"), '""', '"'))

# campaign_de = de.join(campaign, on = 'ip', how = 'inner')

# campaign_de = campaign_de.select('ifa')
# campaign_de.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4001/campaign_de_data/without_vpn/")



# ###################

# de_filtered = de.filter(
#     col("host-info").rlike('"categories":\\s*\\[.*\\b(vpn|proxy)\\b.*\\]')
# )

# campaign_de = de_filtered.join(campaign, on = 'ip', how = 'inner')
# campaign_de = campaign_de.select('ifa')

# campaign_de.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4001/campaign_de_data/with_vpn/")


# ########### run vista report ###############
# wihtout vpn
# cel = spark.read.option('delimiter', '\t').option("header", "true").csv('s3://staging-near-data-analytics/shashwat/ps-4001/campaign_de_data/4344386_4001_campaign_de_without_vpn_expanded_cel_cdl_report_cel.tsv')
# country = cel.groupBy('Common Evening Country').agg(countDistinct('Unhashed Ubermedia Id').alias('ids_cnt'))
# country.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4001/campaign_de_data/report/cel_country_wise_ips/")

# # with vpn filter
# cel = spark.read.option('delimiter', '\t').option("header", "true").csv('s3://staging-near-data-analytics/shashwat/ps-4001/campaign_de_data/4344400_campaign_de_with_vpn_expanded_cel_cdl_report_cel.tsv')
# country = cel.groupBy('Common Evening Country').agg(countDistinct('Unhashed Ubermedia Id').alias('ids_cnt'))
# country.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4001/campaign_de_data/report/cel_country_wise_ips_with_vpn/")


# ####################################
# de = spark.read.option("header", "true").option("delimiter", ";").csv('s3://staging-near-data-analytics/shashwat/ps-4001/de_data/azira_results_2.csv', header=True)
# de = de.withColumn("host-info", regexp_replace(col("host-info"), '""', '"'))

# de_vpn = de.filter(col("host-info").rlike('"categories":\\s*\\[.*\\b(vpn|proxy)\\b.*\\]'))
# de_vpn.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4001/vpn_without_vpn_some_countries/with_vpn_filter/")


# de_not_vpn = de.filter(~col("host-info").rlike('"categories":\\s*\\[.*\\b(vpn|proxy)\\b.*\\]'))
# de_not_vpn.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4001/vpn_without_vpn_some_countries/without_vpn_filter/")



# ###################################

# with_vpn = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-4001/vpn_without_vpn_some_countries/with_vpn_filter/part-00000-ed820a46-793c-4b64-8a95-6bebc36f981b-c000.csv', header = True)
# window_spec = Window.partitionBy("pulseplus-country").orderBy("ip")
# df_with_row_num = with_vpn.withColumn("row_num", row_number().over(window_spec))
# df_countries_5_rows = df_with_row_num.filter(col("row_num") <= 5)
# df_countries_5_rows = df_countries_5_rows.drop("row_num")
# df_countries_5_rows.show(truncate=False)
# df_countries_5_rows.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4001/vpn_without_vpn_5_countries/with_vpn_filter/")

# without_vpn = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-4001/vpn_without_vpn_some_countries/without_vpn_filter/part-00000-dbeb55d4-c84b-40f2-8b9e-ede3c92e9104-c000.csv', header = True)
# window_spec = Window.partitionBy("pulseplus-country").orderBy("ip")
# df_with_row_num = without_vpn.withColumn("row_num", row_number().over(window_spec))
# df_countries_5_rows = df_with_row_num.filter(col("row_num") <= 5)
# df_countries_5_rows = df_countries_5_rows.drop("row_num")
# df_countries_5_rows.show(truncate=False)
# df_countries_5_rows.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4001/vpn_without_vpn_5_countries/without_vpn_filter/")

df_imp = spark.read.parquet("s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=661fe05ffad7b653ba8be138/data_cache/impressions_backup/")

df_vis = spark.read.parquet("s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=661fe05ffad7b653ba8be138/data_cache/visits/location=b8b8a7e4b6f06f6a7ac36c6f3c136a6a/")

df_mat = df_imp.join(df_vis, (df_imp.ifa == df_vis.ifa) & (datediff(df_vis.date, df_imp.date).between(0,90)), "inner").drop(df_imp.ifa).drop(df_imp.date)
df_mat = df_mat.select(['ip','ifa']).dropDuplicates()

with_vpn = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-4001/vpn_without_vpn_5_countries/with_vpn_filter/', header = True)
with_vpn = with_vpn.select('ip').dropDuplicates()
without_vpn = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-4001/vpn_without_vpn_5_countries/without_vpn_filter/', header = True)
with_vpn = with_vpn.select('ip').dropDuplicates()

d1 = df_mat.join(with_vpn, on = 'ip', how = 'inner')
d2 = df_mat.join(without_vpn, on = 'ip', how = 'inner')

d1 = d1.select('ifa').dropDuplicates()
d2 = d2.select('ifa').dropDuplicates()

d1.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4001/temp/with/")
d2.coalesce(1).write.option("header", True).mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-4001/temp/without/")