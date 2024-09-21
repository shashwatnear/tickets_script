import geohash
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import math
from math import radians, sin, cos, asin, sqrt

spark = SparkSession.builder.appName("geohash_to_lat_lon").getOrCreate()

def geohash_to_latlon(geohash_str):
    lat, lon = geohash.decode(geohash_str)
    return (lat, lon)
geohash_to_lat_lon = udf(geohash_to_latlon, StructType([
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False)
]))

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

vista = spark.read.option("sep", "\t").option('header', "true").csv('s3://staging-near-data-analytics/shashwat/acm/files/4318728_distance_travelled_expanded_cel_cdl_report_cel.tsv')
vista = vista.withColumnRenamed('Unhashed Ubermedia Id', 'ifa')

joined = spark.read.parquet('s3://staging-near-data-analytics/shashwat/acm/inner_join_result/*')
'''
{
  "ifa": "125fae4b-824e-46eb-bcaa-c695de188ae6",
  "aspkId": 93971581840,
  "geoHash9": "dhxj10bdg",
  "run_date": "2024-08-09 20:21:07",
  "near_poi_id": "Pollo Tropical 10199, Ft. Lauderdale, FL|8373067"
}
'''
joined = joined.withColumn('lat_lon', geohash_to_lat_lon(col('geoHash9')))
joined = joined.withColumn("lat", col("lat_lon.latitude")).withColumn("lon", col("lat_lon.longitude")).drop("lat_lon")

final = vista.join(joined, on = 'ifa', how = 'inner')
final.write.mode("append").csv(f"s3://staging-near-data-analytics/shashwat/acm/both_lat_lon_file/", header = True)
# +--------------------+------------------+-------------------+----------------------+---------------------+--------------------+---------------------------+--------------------+-----------------------+----------------------+----------------------+----------------------+----------------------+-----------+---------+-------------------+--------------------+------------------+------------------+
# |                 ifa|Common Evening Lat|Common Evening Long|Common Evening Country|Common Evening Census|Common Evening Micro|Common Evening Municipality|Common Evening Admin|Common Evening Province|Common Evening Postal1|Common Evening Postal2|Common Evening Custom1|Common Evening Custom2|     aspkId| geoHash9|           run_date|         near_poi_id|               lat|               lon|
# +--------------------+------------------+-------------------+----------------------+---------------------+--------------------+---------------------------+--------------------+-----------------------+----------------------+----------------------+----------------------+----------------------+-----------+---------+-------------------+--------------------+------------------+------------------+
# |00193248-a1d7-495...|         26.370039|         -80.089941|                   USA|         120990072021|      Boca Raton CCD|       West Palm Beach-F...|   Palm Beach County|                     FL|                 33431|                  null|                  null|                  null|81497691263|dhxn3tj7w|2024-07-30 23:16:18|Pollo Tropical 10...|26.439220905303955|-80.08284330368042|
# |00b67d6f-a117-471...|         27.265450|         -80.380875|                   USA|         121113821104|  Port St. Lucie CCD|       West Palm Beach-F...|    St. Lucie County|                     FL|                 34953|                  null|                  null|                  null|79216707725|dhxp190pw|2024-08-06 22:19:02|Pollo Tropical 10...|26.549770832061768|-80.09005308151245|
# |00b67d6f-a117-471...|         27.265450|         -80.380875|                   USA|         121113821104|  Port St. Lucie CCD|       West Palm Beach-F...|    St. Lucie County|                     FL|                 34953|                  null|                  null|                  null|79216707725|dhxp190n7|2024-08-06 22:16:04|Pollo Tropical 10...|26.549556255340576|-80.09018182754517|
# |00b67d6f-a117-471...|         27.265450|         -80.380875|                   USA|         121113821104|  Port St. Lucie CCD|       West Palm Beach-F...|    St. Lucie County|                     FL|                 34953|                  null|                  null|                  null|79216707725|dhxp190n6|2024-08-06 22:14:38|Pollo Tropical 10...|26.549556255340576| -80.0902247428894|
# +--------------------+------------------+-------------------+----------------------+---------------------+--------------------+---------------------------+--------------------+-----------------------+----------------------+----------------------+----------------------+----------------------+-----------+---------+-------------------+--------------------+------------------+------------------+

# +------------------+-------------------+----------------------+---------------------+--------------------+---------------------------+--------------------+-----------------------+----------------------+----------------------+----------------------+----------------------+-----------+---------+-------------------+--------------------+------------------+------------------+
# |Common Evening Lat|Common Evening Long|Common Evening Country|Common Evening Census|Common Evening Micro|Common Evening Municipality|Common Evening Admin|Common Evening Province|Common Evening Postal1|Common Evening Postal2|Common Evening Custom1|Common Evening Custom2|     aspkId| geoHash9|           run_date|         near_poi_id|               lat|               lon|
# +------------------+-------------------+----------------------+---------------------+--------------------+---------------------------+--------------------+-----------------------+----------------------+----------------------+----------------------+----------------------+-----------+---------+-------------------+--------------------+------------------+------------------+
# |         26.370039|         -80.089941|                   USA|         120990072021|      Boca Raton CCD|       West Palm Beach-F...|   Palm Beach County|                     FL|                 33431|                  null|                  null|                  null|81497691263|dhxn3tj7w|2024-07-30 23:16:18|Pollo Tropical 10...|26.439220905303955|-80.08284330368042|
# |         27.265450|         -80.380875|                   USA|         121113821104|  Port St. Lucie CCD|       West Palm Beach-F...|    St. Lucie County|                     FL|                 34953|                  null|                  null|                  null|79216707725|dhxp190pw|2024-08-06 22:19:02|Pollo Tropical 10...|26.549770832061768|-80.09005308151245|
# |         27.265450|         -80.380875|                   USA|         121113821104|  Port St. Lucie CCD|       West Palm Beach-F...|    St. Lucie County|                     FL|                 34953|                  null|                  null|                  null|79216707725|dhxp190n7|2024-08-06 22:16:04|Pollo Tropical 10...|26.549556255340576|-80.09018182754517|
# |         27.265450|         -80.380875|                   USA|         121113821104|  Port St. Lucie CCD|       West Palm Beach-F...|    St. Lucie County|                     FL|                 34953|                  null|                  null|                  null|79216707725|dhxp190n6|2024-08-06 22:14:38|Pollo Tropical 10...|26.549556255340576| -80.0902247428894|
# +--------------------+------------------+-------------------+----------------------+---------------------+--------------------+---------------------------+--------------------+-----------------------+----------------------+----------------------+----------------------+----------------------+-----------+---------+-------------------+--------------------+------------------+------------------+

df = spark.read.csv('s3://staging-near-data-analytics/shashwat/acm/both_lat_lon_file/part-00000-df9a4bd4-ec98-48c5-939c-ba287765ed47-c000.csv', header = True)
df = df.withColumn('lat', col('lat').cast('float'))
df = df.withColumn('lon', col('lon').cast('float'))
df = df.withColumn('Common Evening Lat', col('Common Evening Lat').cast('float'))
df = df.withColumn('Common Evening Long', col('Common Evening Long').cast('float'))
final = df.withColumn("distance_meters", haversine_udf(col("lat"), col("lon"), col("Common Evening Lat"), col("Common Evening Long")))
final.write.mode("append").csv(f"s3://staging-near-data-analytics/shashwat/acm/ifa_distance/", header = True)

