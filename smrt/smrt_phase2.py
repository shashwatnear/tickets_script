from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import proximityhash
import Geohash
import time
import pyspark.sql.types as T
import hashlib

spark = SparkSession.builder.appName("Tourist cards").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 50, 8)
    return a
generate_geohash_udf = udf(get_geohash)

def computeHash(ID):
    m = hashlib.sha1((ID.upper() + "9o8WnUtwdY").encode())
    return m.hexdigest()

sha_udf = udf(computeHash,StringType())

stay = spark.read.parquet('s3://near-datamart/staypoints/version=*/dataPartner=*/year=2024/month=08/day=*/hour=*/country=SGP/*')
stay = stay.select(['ifa', stay.geoHash9.substr(1,8).alias("geohash"), 'eventDTLocal'])

profiles_file = spark.read.csv('s3://staging-near-data-analytics/shashwat/smrt/august/reports/report_1/', header = True)

# ##################

# Parents seen at kindergarten / preschool i.e. kids from 2 – 6)  - fetch deviceids with profile as “Parents” and who were seen at pois corresponding to category kindergarten/preschool as per places db
poi = spark.read.csv('s3://near-data-analytics/shashwat/smrt/kinder_pre.csv', header = True, sep = '|')
poi = poi.select(poi.geohash.substr(1, 8).alias('geohash'))
kd = stay.join(poi, on = 'geohash', how = 'inner')
kd = kd.select('ifa')
kd.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/smrt/august/phase2_footfall/kinder_pre/")

# ##################

# (Parents seen at primary school i.e. kids 7 – 12 - fetch deviceds with profile as “Parents” and who were seen at pois corresponding to primary school as per places db
poi = spark.read.csv('s3://near-data-analytics/shashwat/smrt/primary_school.csv', header = True, sep = '|')
poi = poi.select(poi.geohash.substr(1, 8).alias('geohash'))
ps = stay.join(poi, on = 'geohash', how = 'inner')
ps = ps.select('ifa')
ps.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/smrt/august/phase2_footfall/primary_school/")

# ##################

# Audience likely to travel overseas - Seen at international airport/terminal in May. Dates seen should be >=1 but less than 10 ( to exclude employees)
poi = spark.read.csv('s3://near-data-analytics/shashwat/smrt/airport_terminal.csv', header = True, sep = '|')
poi = poi.withColumn('geohash', poi.geohash.substr(1, 8).alias('geohash'))
tra = stay.join(poi, on = 'geohash', how = 'inner')
tra.show(10, False)

tra = tra.withColumn('date', col('eventDTLocal').substr(1, 10))
tra = tra.groupBy('name_en', 'ifa').agg(countDistinct('date').alias('occurance'))
tra = tra.withColumn('occurance', col('occurance').cast('int'))
tra = tra.filter((col('occurance') >= 1) & (col('occurance') < 10)).orderBy(col('occurance'))
tra = tra.select('ifa')
tra.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/smrt/august/phase2_footfall/travel_overseas/")

# ##################

# Malaysian working in SGP - Should be seen in for >= 10 days in MYS and SGP
sgp = spark.read.parquet("s3://near-datamart/staypoints/version=*/dataPartner=*/year=2024/month=06/day=*/hour=*/country=SGP/*").select(['ifa', 'eventDTLocal'])
sgp = sgp.withColumn('date', col('eventDTLocal').substr(1, 10))
sgp = sgp.groupBy('ifa').agg(countDistinct('date').alias('occurance'))
sgp = sgp.withColumn('occurance', col('occurance').cast('int'))
sgp = sgp.filter(col('occurance') >= 10).orderBy(col('occurance'))
sgp.show()

mys = spark.read.parquet("s3://near-datamart/staypoints/version=*/dataPartner=*/year=2024/month=06/day=*/hour=*/country=MYS/*").select(['ifa', 'eventDTLocal'])
mys = mys.withColumn('date', col('eventDTLocal').substr(1, 10))
mys = mys.groupBy('ifa').agg(countDistinct('date').alias('occurance'))
mys = mys.withColumn('occurance', col('occurance').cast('int'))
mys = mys.filter(col('occurance') >= 10).orderBy(col('occurance'))
mys.show()

result = sgp.join(mys, on = 'ifa', how = 'inner').drop(sgp['occurance']).drop(mys['occurance']).dropDuplicates()
final = result.select('ifa')
final.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/smrt/august/phase2_footfall/malasian_working_in_sgp/")

# ##################

# Expats - Homelocation in SGP and device language other than English, Mandarin, Malay and Tamil
hl = spark.read.parquet('s3://near-datamart/homeLocation/version=v1/MasterDB/dataPartner=combined/year=2024/month=06/day=16/country=SGP/*')
refined = spark.read.parquet('s3://near-data-warehouse/refined/dataPartner=*/year=2024/month=06/day=*/hour=*/country=SGP/*').select(['ifa', 'devLanguage'])
not_en_zh_ms_ta = refined.where(~col('devLanguage').isin(['en', 'zh', 'ms', 'ta']))

result = not_en_zh_ms_ta.join(hl, on = 'ifa', how = 'inner')
final = result.select('ifa')
final.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/smrt/august/phase2_footfall/expats/")

# ##################

# Blue Collars - Industrial area pois to get from Celia/Manoj
poi = spark.read.csv('s3://near-data-analytics/shashwat/smrt/industrial_zone.csv', header = True, sep = '|')
poi = poi.select(poi.geohash.substr(1, 8).alias('geohash'))
bc = stay.join(poi, on = 'geohash', how = 'inner')
bc = bc.select('ifa')
bc.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/smrt/august/phase2_footfall/blue_collars/")

# ################## Final aggregating with phase1 profiles ####################

location = spark.read.parquet('s3://staging-near-data-analytics/shashwat/smrt/august/footfall_gh8/50m/combined/*').select('ifa')
profiles = spark.read.csv("s3://staging-near-data-analytics/shashwat/smrt/august/reports/report_1/", header = True)

df1 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/smrt/august/phase2_footfall/blue_collars/*')
df1 = df1.withColumn('Profile', lit('blue_collars'))

df2 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/smrt/august/phase2_footfall/malasian_working_in_sgp/*')
df2 = df2.withColumn('Profile', lit('mys_in_sgp'))

df3 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/smrt/august/phase2_footfall/kinder_pre/*')
df3 = df3.withColumn('Profile', lit('kinder_pre'))

df4 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/smrt/august/phase2_footfall/primary_school/*')
df4 = df4.withColumn('Profile', lit('primary_school'))

df5 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/smrt/august/phase2_footfall/expats/*')
df5 = df5.withColumn('Profile', lit('expats'))

df6 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/smrt/august/phase2_footfall/travel_overseas/*')
df6 = df6.withColumn('Profile', lit('travel_overseas'))

combined_profiles = df1.union(df2).union(df3).union(df4).union(df5).union(df6).dropDuplicates()

result = location.join(combined_profiles, on = 'ifa', how = 'inner').select('ifa', 'Profile').dropDuplicates()
result = result.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
result.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/smrt/august/reports/phase2_profiles/", header = True)

# ##################

d1 = spark.read.csv('s3://staging-near-data-analytics/shashwat/smrt/august/reports/report_1/', header = True)
d2 = spark.read.csv('s3://staging-near-data-analytics/shashwat/smrt/august/reports/phase2_profiles/', header = True)
d3 = d1.union(d2)
d3.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/smrt/august/reports/profiles_combined/", header = True)
