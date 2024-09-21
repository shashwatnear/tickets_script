from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash
import hashlib
import argparse

spark = SparkSession.builder.config("spark.driver.maxResultSize", "10g").appName("3932_refined").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

def computeHash(ID):
    m = hashlib.sha1((ID.upper() + "9o8WnUtwdY").encode())
    return m.hexdigest()

sha_udf = udf(computeHash,StringType())

start = "2024-05-01"
end = "2024-05-31"
country = "SGP"

dates = get_dates_between(start, end)
hours = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"]

# for date in dates:
#     for hour in hours:
#         df = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3932/refined/footfall/{}/{}/*".format(date, hour))
#         df = df.withColumn('date', lit(date)).withColumn('hour', lit(hour))
#         grouped_df = df.groupBy('ifa', 'date', 'hour').agg(first('ifa').alias('ff'))
#         grouped_df.write.mode('overwrite').parquet(f's3://staging-near-data-analytics/shashwat/ps-3932/refined/hourly_footfall1/{date}/{hour}/')

# for date in dates:
#     for hour in hours:
#         df = spark.read.parquet(f's3://staging-near-data-analytics/shashwat/ps-3932/refined/hourly_footfall1/{date}/{hour}/*')
#         df = df.select(['ifa', 'ff'])
#         df = df.withColumn('footfall', lit(1)).drop('ff')
#         df.write.mode('overwrite').parquet(f's3://staging-near-data-analytics/shashwat/ps-3932/refined/hourly_footfall2/{date}/{hour}/')

# ###################

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/refined/hourly_footfall2/*/*/*').dropDuplicates()
df = df.groupBy('ifa').agg(sum('footfall').alias('monthly_footfall')).dropDuplicates()
df.write.mode('overwrite').parquet(f's3://staging-near-data-analytics/shashwat/ps-3932/refined/sum_footfall/')


        
# ###################

# df1 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/refined/footfall/*/*').dropDuplicates()
# df2 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/refined/daily_footfall2/*').dropDuplicates()
# result = df1.join(df2, on = 'ifa', how = 'inner').dropDuplicates()
# # result = result.withColumn('Month', date_format(current_date(), "MMMM")).dropDuplicates()
# result = result.withColumn('Month', lit('May'))
# result = result.select(['ifa', 'asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'Month', 'monthly_footfall'])
# result = result.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
# result = result.withColumnRenamed('monthly_footfall', 'Footfall')
# result = result.select(['deviceID', 'asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'Month', 'Footfall'])
# result.coalesce(1).write.mode('overwrite').csv('s3://staging-near-data-analytics/shashwat/ps-3932/report/repot32/', header = True)



# final.coalesce(1).write.mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-3932/reports/refined/report3/", header=True)



# 609,838

# +--------------------+--------------------+--------------------+---------+--------------------+--------------------+-----------+-----------+--------------+
# |                 ifa|             coreSeg|               asset| category|         subCategory|            location|        lat|        lon|total_footfall|
# +--------------------+--------------------+--------------------+---------+--------------------+--------------------+-----------+-----------+--------------+
# |40D2F37F-791C-4A4...|  [2, 5, 7, 93, 132]|Travel - Bus Shel...|AdClassic|Central Region - ...|   Bishan St-Blk 131|1.345346097|103.8512681|             9|
# |40D2F37F-791C-4A4...|  [2, 5, 7, 93, 132]|Travel - Bus Shel...|AdClassic|Central Region - ...|   Bishan St-Blk 131|1.345346097|103.8512681|             9|
# |40D2F37F-791C-4A4...|  [2, 5, 7, 93, 132]|Travel - Bus Shel...|AdClassic|Central Region - ...|   Bishan St-Blk 131|1.345346097|103.8512681|             9|
# |24B65486-3C5F-47E...|             [2, 86]|Travel - Bus Shel...|AdClassic|Central Region - ...|Bishan Rd-Raffles...|1.346061111|103.8465836|             1|
# |104987CB-3778-4AD...|[1, 87, 90, 120, ...|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             6|
# |195DAF41-1FE4-4A4...|[1, 8, 9, 10, 12,...|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             6|
# |1BC13EDA-AB4F-406...|[2, 7, 8, 9, 10, ...|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             1|
# |2AEEE3CC-F8B0-49F...|             [2, 87]|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             1|
# |3D27B4C2-FE9F-496...|                null|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             1|
# |489C7098-AE6B-4CB...|[2, 3, 8, 9, 10, ...|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             3|
# |4E8FC0D1-7536-48B...|                null|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             1|
# |61600B5F-151D-48D...|[1, 8, 9, 10, 12,...|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             1|
# |714F91DA-BC3F-4C4...|[1, 7, 12, 13, 85...|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             2|
# |8BB6B4CD-777D-452...|             [1, 85]|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             6|
# |A1891FC8-468D-489...|   [2, 5, 9, 12, 13]|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             2|
# |A62D5688-016D-48D...|[1, 5, 8, 9, 10, ...|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             1|
# |B26AF102-D94F-4A6...|[1, 4, 8, 9, 10, ...|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             1|
# |BE9BEFF7-61DC-499...|             [2, 85]|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             1|
# |CEA1D8A1-E708-42F...|[1, 7, 8, 9, 10, ...|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             3|
# |D8ABDA2E-BC9E-442...|[2, 8, 9, 10, 12,...|Travel - Bus Shel...|AdClassic|    Northeast Region|Chartwell Dr-Bef ...|1.359763027|103.8636119|             3|
# +--------------------+--------------------+--------------------+---------+--------------------+--------------------+-----------+-----------+--------------+