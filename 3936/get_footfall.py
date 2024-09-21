from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3936").getOrCreate()

# df1 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3936/files/ip_country_city_postcode.csv', header = True)
# df1 = df1.withColumn('postal_code', col('postal_code').cast('int'))

# df2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3936/files/3936.csv')
# df2 = df2.withColumnRenamed('_c0', 'postal_code')
# df2 = df2.withColumn('postal_code', col('postal_code').cast('int'))

# result = df1.join(df2, on = 'postal_code', how = 'inner')
# result = result.groupBy(col('postal_code').alias('zip_codes')).agg(count('ip').alias('impressions'))
# result.coalesce(1).write.mode('append').csv(f"s3://staging-near-data-analytics/shashwat/ps-3936/postcode_ip/", header=True)

# #################################

# df = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3936/files/ip_country_city_postcode.csv', header = True)
# df = df.withColumn('postal_code', col('postal_code').cast('int'))
# df = df.groupBy(col('postal_code').alias('zip_codes')).agg(count('ip').alias('impressions'))
# df.coalesce(1).write.mode('append').csv(f"s3://staging-near-data-analytics/shashwat/ps-3936/all_postcode_wise_ip/", header=True)

# ##################################

# df1 = spark.read.parquet('s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=65e22c0e29ba7472dcf64eb2/data_cache/impressions/*/*')

# df2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3936/files/ip_country_city_postcode.csv', header = True)

# result = df1.join(df2, on = 'ip', how = 'inner')
# result = result.groupBy(col('postal_code').alias('zip_codes')).agg(countDistinct('unique_record_id').alias('impressions'))
# result.coalesce(1).write.mode('append').csv(f"s3://staging-near-data-analytics/shashwat/ps-3936/all_zipcode_wise_uniquerecordid/", header=True)

# ##################################

df1 = spark.read.csv("s3://staging-near-data-analytics/shashwat/ps-3936/all_zipcode_wise_uniquerecordid/", header=True)
df1 = df1.withColumn('postal_code', col('postal_code').cast('int'))

df2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3936/files/3936.csv')
df2 = df2.withColumnRenamed('_c0', 'postal_code')
df2 = df2.withColumn('postal_code', col('postal_code').cast('int'))

df3 = df1.join(df2, on = 'postal_code', how = 'inner')
df3.coalesce(1).write.mode('append').csv(f"s3://staging-near-data-analytics/shashwat/ps-3936/all_zipcode_wise_uniquerecordid_500/", header=True)



