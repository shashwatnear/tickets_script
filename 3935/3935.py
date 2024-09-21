from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from util import *
import proximityhash

spark = SparkSession.builder.appName("3935_h&m_stores").getOrCreate()

# def get_geohash(lat, lon):
#     radius = 30
#     geohash = 8
#     a = proximityhash.create_geohash(float(lat), float(lon), radius, geohash)
#     return a
# generate_geohash_udf = udf(get_geohash)

# places_df = spark.read.csv("s3://staging-near-data-analytics/shashwat/ps-3935/files/h&m_stores.csv", header = True)
# places_df = places_df.withColumn('geohash', generate_geohash_udf(places_df['lat'], places_df['lon']))
# places_df = places_df.withColumn('geohash', split(places_df['geohash'], ','))
# places_df = places_df.withColumn('geohash', explode(places_df['geohash']))

# start = "2024-05-17"
# end = "2024-06-10"
# country = "IND"

# dates = get_dates_between(start, end)

# # Footfall report
# for date in dates:
#     day = "{:02d}".format(date.day)
#     month = '{:02d}'.format(date.month)
#     year = date.year
#     dest_path = f"s3://staging-near-data-analytics/shashwat/ps-3935/footfall/30m/{date}/"

#     data = spark.read.parquet("s3://near-data-warehouse/refined/dataPartner=*/year={}/month={}/day={}/hour=*/country={}/*".format(year,month,day,country))
#     data = data.select(['aspkId', data.eventTs.substr(1,10).alias('loc_time'), data.geoHash9.substr(1, 8).alias('geohash')])
#     ff_df = data.join(places_df, on='geohash', how='inner')
#     ff_df.write.mode("append").parquet(dest_path)

# DOOH EXTRACTION IS IN ANOTHER FILE

# Attributes
eng_df = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3935/dooh_footfall/h&m/*/*")
ff_df = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3935/footfall/30m/*/*")

eng_df = eng_df.select(['aspkId', 'dooh_site', 'loc_time'])
eng_df = eng_df.withColumn('exposed_time', col('loc_time').cast(IntegerType())).drop('loc_time')
eng_df = eng_df.withColumn("exposed_date", to_date(from_unixtime(col("exposed_time")))).drop('exposed_time')

ff_df = ff_df.withColumn('ff_time', col('loc_time').cast(IntegerType())).drop('loc_time')
ff_df = ff_df.withColumn("footfall_date", to_date(from_unixtime(col("ff_time")))).drop('ff_time')

attr_df = eng_df.join(ff_df, eng_df['aspkid'] == ff_df['aspkId']).drop(ff_df.aspkId)
# devices visited the store within 7 days
attr_df = attr_df.withColumn("date_diff", datediff(attr_df["footfall_date"], attr_df["exposed_date"]))
attr_df = attr_df.filter((attr_df["date_diff"] >= 0) & (attr_df["date_diff"] <= 7))
attr_df = attr_df.select(['store_id', 'aspkid'])
attr_df = attr_df.groupBy('store_id').agg(countDistinct('aspkid').alias('footfall'))
attr_df.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3935/store_footfall/30m/", header=True)

# ##############################################################################################################################

# # Total_exposed
# total_exposed = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3935/dooh_footfall/h&m/*/*')
# total_exposed = total_exposed.groupBy('dooh_site').agg(countDistinct('aspkId').alias('total_exposed')).orderBy('dooh_site')
# total_exposed.coalesce(1).write.mode('append').csv(f"s3://staging-near-data-analytics/shashwat/ps-3935/reports/h&m/total_exposed", header=True)

# # 50m
# fifty = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3935/attr_footfall/h&m/50m/*')
# fifty = fifty.groupBy('dooh_List').agg(countDistinct('aspkId').alias('total_total')).orderBy('dooh_List')
# fifty.coalesce(1).write.mode('append').csv(f"s3://staging-near-data-analytics/shashwat/ps-3935/reports/h&m/50m/", header=True)

# # 100m
# fifty = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3935/attr_footfall/h&m/100m/*')
# fifty = fifty.groupBy('dooh_List').agg(countDistinct('aspkId').alias('total_total')).orderBy('dooh_List')
# fifty.coalesce(1).write.mode('append').csv(f"s3://staging-near-data-analytics/shashwat/ps-3935/reports/h&m/100m/", header=True)

# =VLOOKUP(D2, A:B, 2, FALSE)

# =INDEX(H:H, MATCH(B2, I:I, 0))