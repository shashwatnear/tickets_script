from pyspark.sql import SparkSession
import geohash
from polygon_geohasher.polygon_geohasher import polygon_to_geohashes, geohashes_to_polygon
import json
from shapely.geometry import shape, mapping
import numpy as np
import pyspark.sql.functions as F
import pyspark.sql.types as T
import s3fs
import boto3
from datetime import datetime, timedelta
from util import *

spark = SparkSession.builder.appName("campaign_details").getOrCreate()


#coverting json into geohashs.............


def read_geojson_polygons(spark, geojson_path):
    """
    Read polygon json file in distributed format
    """
    poly_spdf = spark.read.json(geojson_path, multiLine="True")
    poly_spdf.printSchema()
    poly_spdf = poly_spdf.withColumn("geom", F.explode(poly_spdf.features)).drop("features")
    poly_spdf = poly_spdf.filter(poly_spdf.geom.geometry.isNotNull())
    poly_spdf = poly_spdf.drop("_corrupt_record")

    poly_spdf = poly_spdf.withColumn(
        "coordinates",
        correct_polygon_geometry_schema_udf(poly_spdf.geom.geometry.coordinates),
    )
    poly_spdf = poly_spdf.withColumn("type", F.lit("MultiPolygon"))
    poly_spdf = poly_spdf.withColumn("new_geom", F.struct(F.col("coordinates"), F.col("type")))
    print("poly count : {}".format(poly_spdf.count()))
    poly_spdf.show()
    poly_spdf = poly_spdf.drop("geometry")
    poly_spdf = poly_spdf.withColumnRenamed("new_geom", "geometry")
    return poly_spdf

def correct_polygon_geometry_schema(geometry_coordinates):
    coords_string = json.dumps(geometry_coordinates)
    coords_string = coords_string.replace('"', "")
    if (coords_string[0:4] == "[[[[") & (coords_string[0:5] != "[[[[["):
        coords_array = json.loads(coords_string)
    elif (coords_string[0:3] == "[[[") & (coords_string[0:4] != "[[[["):
        coords_array = [json.loads(coords_string)]
    else:
        coords_array = None
    if coords_array is not None:
        coords_array = [
            ring_geom_cast(i) if len(i) > 1 else simple_polygon_geom(i)
            for i in coords_array
        ]
    return coords_array

def ring_geom_cast(coords):
    coords_array = [np.asarray(c).astype(float).tolist() for c in coords]
    return coords_array

def simple_polygon_geom(coords):
    coords_array = np.asarray(coords)
    coords_array = coords_array.astype(float).tolist()
    return coords_array

def get_approx_polygon_area(geom):
    geom_text = {"coordinates": geom.coordinates, "type": geom.type}
    s = json.dumps(geom_text)
    g1 = json.loads(s)
    geom = shape(g1)
    area = geom.area * (111000 * 111000)
    return area

def get_optimum_geohash_level(polygon_area):
    gh_area_lvl = {
        9: 22.752899999999997,
        8: 729.6200000000001,
        7: 23409,
        6: 744199.9999999999,
        5: 23912100.0,
        4: 762450000.0,
        3: 24336000000,
        2: 781250000000,
        1: 25000000000000,
    }
    optimum_lvl = None
    for k in gh_area_lvl.keys():
        no_of_possible_gh = polygon_area / gh_area_lvl[k]
        if no_of_possible_gh <= 10000:
            optimum_lvl = k
            break
    return optimum_lvl

def polygon_to_geohash(geom, geohash_lvl):
    geom_text = {"coordinates": geom.coordinates, "type": geom.type}
    s = json.dumps(geom_text)
    g1 = json.loads(s)
    geom = shape(g1)
    try:
        lst = list(polygon_to_geohashes(geom, geohash_lvl, False))
    except:
        lst = list()
    return lst

# Register UDFs
correct_polygon_geometry_schema_udf = F.udf(
    correct_polygon_geometry_schema,
    T.ArrayType(T.ArrayType(T.ArrayType(T.ArrayType(T.DoubleType())))),
)
get_approx_polygon_area_udf = F.udf(get_approx_polygon_area, T.DoubleType())
get_optimum_geohash_level_udf = F.udf(get_optimum_geohash_level, T.IntegerType())
polygon_to_geohash_udf = F.udf(polygon_to_geohash, T.ArrayType(T.StringType()))

def fetch_places(spark, input_path, output_path):
    gh_data = read_geojson_polygons(spark, input_path)
    gh_data = gh_data.withColumn("poly_area", get_approx_polygon_area_udf(F.col("geometry")))
    gh_data = gh_data.withColumn("gh_level", get_optimum_geohash_level_udf(F.col("poly_area")))
    gh_data = gh_data.withColumn("geohash_list", polygon_to_geohash_udf(F.col("geometry"), F.col("gh_level")))

    places = gh_data.withColumn("geohash", F.explode(F.col("geohash_list")))
    places = places.withColumn("near_poi_id", places.geom.id)

# here u can add geohash level rishi rishii
    places = places.select("near_poi_id", "geohash").distinct()
    places.coalesce(10).write.parquet(output_path, mode="overwrite")



#getting visits...............

# start_date_str = "2024-01-15"
# end_date_str = "2024-05-15"
# country = "USA"
# start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
# end_date = datetime.strptime(end_date_str, "%Y-%m-%d")


# input_path = "s3://staging-near-data-analytics/shashwat/ps-4010/files/Mill Casino and Hotel_6595c63b5220ef2f7dce72e4.json"
# output_path = "s3://staging-near-data-analytics/shashwat/ps-4010/campaign_data/places_geohash/Oregon/"
# fetch_places(spark, input_path, output_path)

start = "2024-01-15"
end = "2024-08-31"
country = "USA"

dates = get_dates_between(start, end)

poi = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-4010/campaign_data/places_geohash/Oregon/*')

# Footfall report
for date in dates:
    day = "{:02d}".format(date.day)
    month = '{:02d}'.format(date.month)
    year = date.year
    dest_path = "s3://staging-near-data-analytics/shashwat/ps-4010/campaign/footfall/{}/".format(date)

    data = spark.read.parquet(f"s3://near-data-warehouse/refined/dataPartner=*/year={year}/month={month}/day={day}/hour=*/country=USA/*")
    data = data.select(['ifa', data.geoHash9.substr(1, 9).alias('geohash')])
    ff_df = data.join(poi, on='geohash', how='inner').drop('geohash')
    ff_df.coalesce(5).write.mode("append").parquet(dest_path)
