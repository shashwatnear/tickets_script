from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from util import *
from pyspark.sql.functions import row_number
from pyspark.sql.window import *


spark = SparkSession.builder.appName("3976").getOrCreate()

# Extracting aspkIds for month of july for 9 segments. 

segId = [87782, 87775, 87774, 87772, 87761, 87755, 87748, 87746, 87743]

# start = '2024-07-01'
# end = '2024-07-20'

# dates = get_dates_between(start, end)

# for date in dates:
#   try:
#     for i in segId:
#         aa = spark.read.parquet(f"s3://near-rule-engine-data/rule-output/segment={i}/date={date}/")
#         aa1 = aa.select("aspkId").distinct()
#         aa1 = aa1.withColumn("segId", lit(i))
#         aa1 = aa1.withColumn("date", lit(date))
#         aa1.write.parquet(f"s3://staging-near-data-analytics/shashwat/ps-3976/postcode/aspk_july/{i}/{date}/")
#         print(i)
#   except:
#     print(date, " Path does not exists")

# ##################
# aa = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3976/postcode_final/ifa_segId_aspkId_july/*")
# start = '2024-07-01'
# end = '2024-07-20'

# dates = get_dates_between(start, end)

# for date in dates:
#   try:
#     for i in segId:
#         aa1 = spark.read.parquet(f"s3://near-rule-engine-data/rule-output/segment={i}/date={date}/")
#         aa1 = aa1.select("aspkId").distinct()
#         aa1 = aa1.join(aa, on = 'aspkId', how = 'inner')
#         aa1 = aa1.withColumn("date", lit(date))
#         aa1.write.parquet(f"s3://staging-near-data-analytics/shashwat/ps-3976/postcode/ifa_segId_aspkId_july/{i}/{date}/")
#         print(i)
#   except:
#     print(date, " Path does not exists")

# ##################
    

# # Joining aspkId to rule-engine and getting the segId<>ifa<>aspkid mapping

# #segId<>ifa<>aspkid mapping using id-manager dump#
# aa = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3976/postcode/aspk_july/*/*/*")

# #id_man = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3976/id_man_dump_2/")
# id_man = spark.read.parquet("s3://near-data-warehouse/id-manager-offline/current/")
# id_man = id_man.select("aspk_id", "ifa").distinct()

# final = aa.join(id_man, aa.aspkId == id_man.aspk_id, "inner").drop(id_man.aspk_id)
# final.write.parquet("s3://staging-near-data-analytics/shashwat/ps-3976/postcode/ifa_segId_aspkId_july/")

# #################

# aa = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3976/postcode/ifa_segId_aspkId_july/*/*")
# '''
# {
#   "aspkId": 73072128249,
#   "segId": 87775,
#   "ifa": "535C64C8-E57C-49AD-A688-6FB44AA5A8D6"
# }
# '''
# aa  = aa.withColumn("ifa", upper(col("ifa")))

# start = '2024-07-01'
# end = '2024-07-20'

# dates = get_dates_between(start, end)

# for date in dates:
#   try:
#     bb = spark.read.parquet(f's3://near-datamart/universal_cross_matrix/measurement/country=AUS/date={date}/*')
#     bb = bb.withColumn('date', lit(date))
#     '''
#     {
#       "ifa": "0F2426A4-CB15-47A9-818B-91103C3B2300",
#       "ip": "203.206.130.37"
#     }
#     '''
#     bb = bb.withColumn('ifa', upper(col('ifa')))
#     cc = aa.join(bb, on = ['ifa', 'date'], how = 'inner')
#     #writing the data#
#     cc = cc.select("ip", "ifa", 'date').distinct()
#     cc.write.parquet(f"s3://staging-near-data-analytics/shashwat/ps-3976/postcode/ip_ifa/{date}/")
#   except:
#     print(date, " Path does not exists")

# #################

# get maxmind data locally

# #################

df = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3976/postcode_final/ip_ifa_state_postcode/part-00000-6d26b6ba-fcd7-49f4-a991-56c42605c987-c000.csv', header = True)
df1 = df.groupBy('state').agg(countDistinct('ip').alias('cnt'))
df2 = df.groupBy('postal_code').agg(countDistinct('ip').alias('cnt'))
df1.coalesce(1).write.mode("append").csv("s3://staging-near-data-analytics/shashwat/ps-3976/postcode_final/report/ip_state_cnt/", header = True)
df2.coalesce(1).write.mode("append").csv("s3://staging-near-data-analytics/shashwat/ps-3976/postcode_final/report/ip_pc_cnt/", header = True)


