from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3979_rule_vs_stay").getOrCreate()

def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 1609, 7)
    return a
generate_geohash_udf = udf(get_geohash)

start = "2024-06-01"
end = '2024-06-30'

dates = get_dates_between(start, end)

poi = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3979/files/3979.csv', header = True)
poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
poi = poi.withColumn('geohash',explode('geohash'))
poi.show()

stay = spark.read.parquet(f's3://near-datamart/staypoints/version=v4/dataPartner=*/year=2024/month=06/day=*/hour=*/country=CAN/*')
stay = stay.filter(col('hdp_flag') == 0)
stay = stay.select(['aspkId', stay.geoHash9.substr(1,7).alias('geohash')])
stay = stay.join(poi, on='geohash', how='inner')
stay = stay.select('aspkId')
stay.write.mode("overwrite").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3979/rule_vs_stay/stay_again/")

rule = spark.read.parquet('s3://near-rule-engine-data/rule-input/CAN/2024-06-*/*')
rule = rule.filter(col('hdp_flag') == 0)
rule = rule.select(['aspkId', rule.GH8.substr(1,7).alias('geohash')])
rule = rule.join(poi, on='geohash', how='inner')
rule = rule.select('aspkId')
rule.write.mode("overwrite").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3979/rule_vs_stay/rule_again/")

# ###################

df1 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3979/rule_vs_stay/rule_again/*')
df2 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3979/rule_vs_stay/stay_again/*')

unmatched_ids = df1.join(df2, on="aspkId", how="full_outer") \
                   .filter(df1["aspkId"].isNull() | df2["aspkId"].isNull()) \
                   .selectExpr("coalesce(aspkId, aspkId) as aspkId")

df1.agg(countDistinct('aspkId')).show()
# 6,382,490
df2.agg(countDistinct('aspkId')).show()
# 1,473,913
unmatched_ids.agg(countDistinct('aspkId')).show()
# 4,908,703
# unmatched_ids.show()


# updated
# rule - 5,503,630
# stay - 1,473,913
# unmatched_ids - 4,029,717