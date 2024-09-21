from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("sample_refined").getOrCreate()

def get_geohash(lat, lon, radius):
    a = proximityhash.create_geohash(float(lat), float(lon), int(radius), 9)
    return a
generate_geohash_udf = udf(get_geohash)

# poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3992/files/3992_15_28.csv', header = True)
# poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon'), col('radius')))
# poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
# poi = poi.withColumn('geohash',explode('geohash'))

# start = "2024-07-15"
# end = "2024-07-21"
# country = "TUR"

# dates = get_dates_between(start, end)

# # Footfall refined
# for date in dates:
#     day = "{:02d}".format(date.day)
#     month = '{:02d}'.format(date.month)
#     year = date.year

#     data = spark.read.parquet(f"s3://near-data-warehouse/refined/dataPartner=*/year=2024/month=07/day={day}/hour=*/country=TUR/*")
#     data = data.select(['aspkId', 'coreSeg', data.geoHash9.substr(1, 9).alias('geohash')])
#     ff_df = data.join(poi, on='geohash', how='inner').drop('geohash')
#     ff_df.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3992/poi_footfall/sample_refined/15_21/{}/".format(date))

# ################################

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3992/poi_footfall/sample_refined/15_21/*/*')
demog_df = spark.read.csv('s3://near-data-analytics/Ajay/coreSeg.csv', header=True)

df = df.withColumn('coreSeg', explode_outer('coreSeg')).drop_duplicates()
df = df.join(demog_df, on='coreSeg').drop('coreSeg')

df.agg(countDistinct('aspkId')).show()
df.filter(col('desc') == 'Male').agg(countDistinct('aspkId')).show()
df.filter(col('desc') == 'Female').agg(countDistinct('aspkId')).show()
df.filter(~col('desc').isin(['Male', 'Female'])).agg(countDistinct('aspkId')).show()