from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash
import hashlib

spark = SparkSession.builder.appName("3932_final").getOrCreate()

start = "2024-05-01"
end = "2024-05-31"
country = "SGP"

dates = get_dates_between(start, end)

# UDFs
def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 50, 8)
    return a
generate_geohash_udf = udf(get_geohash)

def computeHash(ID):
    m = hashlib.sha1((ID.upper() + "9o8WnUtwdY").encode())
    return m.hexdigest()

sha_udf = udf(computeHash,StringType())

# -------------------------------------------------------------

# poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3932/files/SMRT-Dashboard location master v1 - Sheet1.csv', header = True)
# poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
# poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
# poi = poi.withColumn('geohash',explode('geohash'))
# poi.show()

# # Footfall report
# for date in dates:
#     try:
#         day = "{:02d}".format(date.day)
#         month = '{:02d}'.format(date.month)
#         year = date.year
#         dest_path = "s3://staging-near-data-analytics/shashwat/ps-3932/final/refined/footfall_gh8/{}/".format(date)

#         data = spark.read.parquet(f"s3://near-data-warehouse/refined/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country={country}/*")
#         data = data.select(['ifa', data.geoHash9.substr(1,8).alias('geohash'), 'coreSeg', 'eventDTLocal'])
#         ff_df = data.join(poi, on='geohash', how='inner').drop('geohash')
#         ff_df.write.mode("overwrite").parquet(dest_path)
#     except:
#         print(date, " Path does not exists")

# #########################
# report 1
df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/final/refined/footfall_gh8/*/*')
demog_df = spark.read.csv('s3://near-data-analytics/Ajay/coreSeg.csv', header=True)
df = df.withColumn('ifa', upper('ifa'))
result = df.select(['ifa','coreSeg']).dropDuplicates()
result = result.withColumn('coreSeg', explode_outer('coreSeg')).drop_duplicates()
result = result.join(demog_df, on='coreSeg').drop('coreSeg')
result = result.withColumnRenamed('desc', 'Profile')
result = result.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
result = result.select(['deviceID', 'Profile'])

# now profiles for 1st report
profiles = result.filter((col('Profile') == 'Affluents') | (col('Profile') == 'Students') | (col('Profile') == 'Shoppers') | (col('Profile') == 'Parents') | (col('Profile') == 'Professionals'))

# early retirement
pro = result.filter(col('Profile') == 'Professionals')
ff = result.filter(col('Profile') == '45-54')
early_retirement = pro.join(ff, on = 'deviceID', how = 'inner')
early_retirement = early_retirement.select('deviceID').withColumn('Profile', lit('Early_Retirement'))

# retirement
fifty_five_plus = result.filter(col('Profile') == '55+')
retirement = pro.join(fifty_five_plus, on = 'deviceID', how = 'inner')
retirement = retirement.select(['deviceID']).withColumn('Profile', lit('Retirement_State'))

final = profiles.union(early_retirement).union(retirement).dropDuplicates()
final.coalesce(1).write.mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-3932/final/report_gh8/report_1/", header = True)

# #########################
# report 2
df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/final/refined/footfall_gh8/*/*')
demog_df = spark.read.csv('s3://near-data-analytics/Ajay/coreSeg.csv', header=True)
df = df.withColumn('ifa', upper('ifa'))
result = df.select(['ifa','coreSeg']).dropDuplicates()
result = result.withColumn('coreSeg', explode_outer('coreSeg')).drop_duplicates()
result = result.join(demog_df, on='coreSeg').drop('coreSeg')
result = result.withColumnRenamed('desc', 'Profile')
result = result.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
result = result.select(['deviceID', 'Profile'])

gender = result.filter((col('Profile') == 'Male') | (col('Profile') == 'Female'))
gender = gender.withColumnRenamed('Profile', 'Gender')

age = result.filter((col('Profile') == '18-24') | (col('Profile') == '25-34') | (col('Profile') == '35-44') | (col('Profile') == '45-54') | (col('Profile') == '55+'))
age = age.withColumnRenamed('Profile', 'Age')

result = gender.join(age, on = 'deviceID', how = 'inner')
result.coalesce(1).write.mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-3932/final/report_gh8/report_2/", header=True)

# #########################
# report 3
df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/final/refined/footfall_gh8/*/*')
df = df.withColumn('Month', lit('May'))
df = df.withColumn("hour", substring(col("eventDTLocal"), 12, 2)).drop(col('eventDTLocal'))
df = df.groupBy('ifa', 'asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'Month').agg(count('hour').alias('Footfall'))
df = df.withColumn('Footfall', col('Footfall').cast('int'))
df = df.filter(col('Footfall') < 151)
df = df.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
df = df.select(['deviceID', 'asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'Month', 'Footfall'])
df.coalesce(1).write.mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-3932/final/report_gh8/report_3/", header=True)

# #########################


