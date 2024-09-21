from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash
import hashlib

spark = SparkSession.builder.appName("smrt_phase1").getOrCreate()

start = "2024-08-01"
end = "2024-08-31"
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

# ----------------------------- Get POI footfall --------------------------------

poi = spark.read.csv('s3://near-data-analytics/shashwat/smrt/SMRT-Dashboard location master v1 - Sheet1.csv', header = True)
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))
poi.show()

# Footfall report
for date in dates:
    try:
        day = "{:02d}".format(date.day)
        month = '{:02d}'.format(date.month)
        year = date.year
        dest_path = "s3://staging-near-data-analytics/shashwat/smrt/august/footfall_gh8/50m/{}/".format(date)
        print(dest_path)
        data = spark.read.parquet(f"s3://near-datamart/staypoints/version=*/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country={country}/*")
        data = data.where(col("hdp_flag")==0)
        data = data.select(['ifa', data.geoHash9.substr(1,8).alias('geohash'), 'coreSeg', 'eventDTLocal'])
        ff_df = data.join(poi, on='geohash', how='inner')
        ff_df.write.mode("overwrite").parquet(dest_path)
    except:
        print(date, " Path does not exists")

# ######################### Remove duplicate profile IDs #############################
        
df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/smrt/august/footfall_gh8/50m/*/*')
print("Before")
df.agg(countDistinct('ifa')).show()

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

final.write.mode('append').parquet("s3://staging-near-data-analytics/shashwat/smrt/august/footfall_gh8/50m/combined/")


# ######################### Profile #####################
# report 1
df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/smrt/august/footfall_gh8/50m/combined/*')
demog_df = spark.read.csv('s3://near-data-analytics/Ajay/coreSeg.csv', header=True)

df = df.withColumn('ifa', upper(col('ifa')))
result = df.select(['ifa', 'coreSeg'])
result = result.withColumn('coreSeg', explode_outer(col('coreSeg')))

result = result.join(demog_df, on='coreSeg').drop('coreSeg')

result = result.withColumnRenamed('desc', 'Profile')

result = result.select(['ifa', 'Profile'])

profiles = result.filter(col('Profile').isin('Affluents', 'Students', 'Shoppers', 'Parents', 'Professionals'))

pro = result.filter(col('Profile') == 'Professionals')

# Early retirement: join Professionals with age 45-54
ff = result.filter(col('Profile') == '45-54')
early_retirement = pro.join(ff, on='ifa', how='inner').select('ifa').withColumn('Profile', lit('Early_Retirement'))

# Retirement: join Professionals with age 55+
fifty_five_plus = result.filter(col('Profile') == '55+')
retirement = pro.join(fifty_five_plus, on='ifa', how='inner').select('ifa').withColumn('Profile', lit('Retirement_State'))

# Combine all profiles and remove duplicates
final = profiles.union(early_retirement).union(retirement).dropDuplicates()
final = final.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
final.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/smrt/august/reports/report_1/", header = True)
print("report1 created")

# ######################### Age gender ########################
# report 2
df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/smrt/august/footfall_gh8/50m/combined/*')
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
result.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/smrt/august/reports/report_2/", header=True)
print("report2 created")
# # ######################### Location #########################
# report 3
df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/smrt/august/footfall_gh8/50m/combined/*')
df = df.withColumn('Month', lit('August'))
df = df.withColumn("hour", substring(col("eventDTLocal"), 12, 2)).drop(col('eventDTLocal'))
df = df.groupBy('ifa', 'asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'Month').agg(count('hour').alias('Footfall'))
df = df.withColumn('Footfall', col('Footfall').cast('int'))
df = df.filter(col('Footfall') < 151)
df = df.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
df = df.select(['deviceID', 'asset', 'category', 'subCategory', 'location', 'lat', 'lon', 'Month', 'Footfall'])
df.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/smrt/august/reports/report_3/", header=True)
print("report3 created")
# # #########################


