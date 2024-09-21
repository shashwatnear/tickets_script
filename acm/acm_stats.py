from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from util import *

spark = SparkSession.builder.appName("match_client_promixa").getOrCreate()

# print('near universe IDs')
# near_universe = spark.read.csv('s3://staging-near-data-analytics/shashwat/acm/files/Near_All_Hems_News_Mar2024.csv', header = True)
# near_universe = near_universe.withColumn('hems', lower('hems'))
# near_universe.agg(countDistinct('hems')).show()

# print('client shared IDs')
# client_hems = spark.read.csv('s3://staging-near-data-analytics/shashwat/acm/files/hashed_emails_near_2024.csv', header = True)
# client_hems = client_hems.withColumn('hashed_email', lower('hashed_email'))
# client_hems.agg(countDistinct('hashed_email')).show()

# print('near universe & client common IDs')
# near_client_matched = spark.read.csv('s3://staging-near-data-analytics/shashwat/acm/near_client_hems_match/part-00000-ffaf8a77-bd18-4cc5-b62d-7dcd36fd8163-c000.csv', header = True)
# near_client_matched = near_client_matched.withColumn('hems', lower('hems'))
# near_client_matched.agg(countDistinct('hems')).show()

# print('proxima MAIDs')
# proxima_ids = spark.read.csv('s3://staging-near-data-analytics/shashwat/acm/maids/promixa/part-00000-320164c7-cbac-4547-a2b7-0c38b2b92485-c000.csv', header = True)
# proxima_ids = proxima_ids.withColumn('maids', upper('maids'))
# proxima_ids.agg(countDistinct('maids')).show()

# print('cross matrix IFAs')
# cross_matrix = spark.read.csv('s3://staging-near-data-analytics/shashwat/acm/maids/cross_matrix/part-00000-8446d4a6-4043-4b7a-9706-972a062f6f73-c000.csv', header = True)
# cross_matrix = cross_matrix.withColumn('ifa', upper('ifa'))
# cross_matrix.agg(countDistinct('ifa')).show()

# ######## maids hems distribution ########

# df = spark.read.json('s3://staging-near-data-analytics/shashwat/acm/proxima/SuAXjveS/*')
# df = df.select(['hems', 'maids'])
# df = df.withColumn('hems', explode(df.hems)).withColumn('maids', explode(df.maids))
# df = df.withColumn('hems', lower(df.hems))
# df = df.withColumn('maids', lower(df.maids))

# df.agg(countDistinct('maids')).show()
# 28,645,524

# df.agg(countDistinct('hems')).show()
# 20,971,039

# df.groupBy('hems').agg(countDistinct('maids').alias('maids_cnt')).orderBy('maids_cnt', ascending = False).show()
# max count = 3

# https://edmdls.com/martin-garrix-x-julian-jordan-x-tinie-tempah-diamonds-extended-mix/