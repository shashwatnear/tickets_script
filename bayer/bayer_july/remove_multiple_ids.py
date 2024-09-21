from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash
import hashlib

spark = SparkSession.builder.appName("3966_final").getOrCreate()

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/bayer/june/footfall_gh8/50m/*/*')
print("Before")
df.agg(countDistinct('ifa')).show()
# 787725
# 787677
result = df.withColumn('coreSeg', explode_outer(col('coreSeg')))
demog_df = spark.read.csv('s3://near-data-analytics/Ajay/coreSeg.csv', header=True)

result = result.join(demog_df, on='coreSeg').drop('coreSeg')
result = result.withColumnRenamed('desc', 'Profile')

result = result.filter(col('Profile').isin(['18-24', '25-34', '35-44', '45-54', '55+']))

# Group by 'id' and count distinct 'profile' values
profile_counts = result.groupBy("ifa").agg(countDistinct("Profile").alias("profile_count"))

# Filter to find IDs with more than one profile
multiple_profiles_df = profile_counts.filter(col("profile_count") > 1)

# Select the IDs with multiple profiles
ids_with_multiple_profiles = multiple_profiles_df.select("ifa")

final = df.join(ids_with_multiple_profiles, on = 'ifa', how = 'left_anti')
print("After")
final.agg(countDistinct('ifa')).show()

final.write.mode('append').parquet("s3://staging-near-data-analytics/shashwat/bayer/june/footfall_gh8/50m/combined/")
# ids_with_multiple_profiles.show()