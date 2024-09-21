import geohash
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import math
from math import radians, sin, cos, asin, sqrt
import argparse
from datetime import datetime

todays_date = datetime.today().strftime('%Y-%m-%d')

spark = SparkSession.builder.appName("pollo tropical automation part2").getOrCreate()

# Utility function to convert geohash to lat/lon
def geohash_to_latlon(geohash_str):
    lat, lon = geohash.decode(geohash_str)
    return (lat, lon)
geohash_to_lat_lon = udf(geohash_to_latlon, StructType([
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False)
]))

# Utility function to calculate distance using haversine formula
def haversine(lat1, lon1, lat2, lon2):
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    # Calculate the differences
    dlat = lat2 - lat1
    dlon = lon2 - lon1


    # Haversine formula
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))

    # Earth's radius in meters
    r = 6371000

    return c * r
haversine_udf = udf(haversine)

def calculate_median(values_list):
    sorted_list = sorted(values_list)
    length = len(sorted_list)
    if length % 2 == 0:
        return float((sorted_list[length // 2 - 1] + sorted_list[length // 2]) / 2.0)
    else:
        return float(sorted_list[length // 2])
median_udf = udf(calculate_median, FloatType())

# Function to calculate distances between locations
def calculate_distances(vista_path, result_path, todays_date):
    vista = spark.read.option("sep", "\t").option('header', "true").csv(vista_path).withColumnRenamed('Unhashed Ubermedia Id', 'ifa')

    # overall vista ids count
    vista_ids = vista.agg(countDistinct('ifa'))
    vista_ids.write.mode("append").csv(f"s3://staging-near-data-analytics/shashwat/pollo_tropical/{todays_date}/overall/vista_ids/", header=True)
    # #######################

    joined = spark.read.parquet(f's3://staging-near-data-analytics/shashwat/pollo_tropical/{todays_date}/inner_join_result/*')
    
    # overall attr ids count
    joined_ids = joined_ids.agg(countDistinct('ifa'))
    joined_ids.write.mode("append").csv(f"s3://staging-near-data-analytics/shashwat/pollo_tropical/{todays_date}/overall/attr_ids/", header=True)
    # ######################

    joined = joined.withColumn('lat_lon', geohash_to_lat_lon(col('geoHash9')))
    joined = joined.withColumn("lat", col("lat_lon.latitude")).withColumn("lon", col("lat_lon.longitude")).drop("lat_lon")

    final = vista.join(joined, on = 'ifa', how = 'inner').dropDuplicates()

    # overall attr ids cel ids count
    final_ids = final_ids.agg(countDistinct('ifa'))
    final_ids.write.mode("append").csv(f"s3://staging-near-data-analytics/shashwat/pollo_tropical/{todays_date}/overall/attr_cel_ids/", header=True)
    # #############

    # ######## Overall median ##########
    values_list = final.select("value").rdd.flatMap(lambda x: x).collect()
    values_list.sort()
    length = len(values_list)
    if length % 2 == 0:
        overall_median = (values_list[length // 2 - 1] + values_list[length // 2]) / 2.0
    else:
        overall_median = values_list[length // 2]

    print(f"Overall Median: {overall_median}")
    # #################################

    poi_wise_ifa_count = final.groupBy('near_poi_id').agg(countDistinct('ifa').alias('ifa_cnt'))
    poi_wise_ifa_count.write.mode("append").csv(f"s3://staging-near-data-analytics/shashwat/pollo_tropical/{todays_date}/poi_wise_ifa_count/", header=True)

    final = final.withColumn('lat', col('lat').cast('float'))
    final = final.withColumn('lon', col('lon').cast('float'))
    final = final.withColumn('Common Evening Lat', col('Common Evening Lat').cast('float'))
    final = final.withColumn('Common Evening Long', col('Common Evening Long').cast('float'))

    final = final.withColumn("distance_meters", haversine_udf(col("lat"), col("lon"), col("Common Evening Lat"), col("Common Evening Long")))
    final.write.mode("append").csv(result_path, header=True)

def calculate_avg_median(path, todays_date):
    df = spark.read.csv(path, header = True)
    df = df.withColumn('distance_meters', col('distance_meters').cast('float'))

    avg_df = df.groupBy('near_poi_id').agg(avg('distance_meters').alias('average'))

    median_df = df.groupBy("near_poi_id").agg(collect_list("distance_meters").alias("values_list")) \
                .withColumn("median", median_udf(col("values_list"))) \
                .select("near_poi_id", "median")
    result_df = avg_df.join(median_df, on="near_poi_id")
    result_df.coalesce(1).write.mode("append").csv(f"s3://staging-near-data-analytics/shashwat/pollo_tropical/{todays_date}/poi_wise_avg_median/", header = True)

calculate_distances(f"s3://staging-near-data-analytics/shashwat/pollo_tropical/{todays_date}/files/*.tsv", 
                    f"s3://staging-near-data-analytics/shashwat/pollo_tropical/{todays_date}/ifa_distance/", todays_date)
calculate_avg_median(f"s3://staging-near-data-analytics/shashwat/pollo_tropical/{todays_date}/ifa_distance/*", todays_date)