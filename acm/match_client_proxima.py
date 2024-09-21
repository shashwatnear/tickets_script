from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from util import *

spark = SparkSession.builder.appName("match_client_promixa").getOrCreate()

client = spark.read.csv('s3://staging-near-data-analytics/shashwat/acm/files/hashed_emails_near_2024.csv', header = True)
near = spark.read.csv('s3://staging-near-data-analytics/shashwat/acm/files/Near_All_Hems_News_Mar2024.csv', header = True)
matching_hems = near.join(client, near.hems == client.hashed_email, "inner")
matching_hems = matching_hems.select('hems').dropDuplicates()
matching_hems.coalesce(1).write.mode("append").csv("s3://staging-near-data-analytics/shashwat/acm/near_client_hems_match/", header = True)
# matching ids = 498,579


# start = '2024-05-01'
# end = '2024-08-30'

# dates = get_dates_between(start, end)

# for date in dates:
#     try:
#         day = "{:02d}".format(date.day)
#         month = '{:02d}'.format(date.month)
#         year = date.year
#         cross = spark.read.parquet(f's3://near-datamart/universal_cross_matrix/compass/linkages/country=AUS/tenant_id=b54be51/datasource_id=2530/year=2024/month={month}/day={day}/*').select(['from', 'to'])
#         cross = cross.withColumnRenamed('from', 'ifa').withColumnRenamed('to', 'ncid')
#         cross.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/acm/cross_matrix_90days/{date}/")
#     except Exception as e:
#         print(e)