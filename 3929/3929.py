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





def fetch_data_partners_from_s3(staypoints_version):
    s3_client = boto3.client("s3")
    data_partners_list = []
    response = s3_client.list_objects_v2(
        Bucket=staypoints_bucket,
        Prefix="staypoints/version={}/".format(staypoints_version),
        Delimiter="/",
    )
    print(f"Fetching data partners for version {staypoints_version}:")
    for content in response.get("CommonPrefixes", []):
        data_partner = content.get("Prefix").split("/")[-2].split("=")[1]
        print(f"Data partner found: {data_partner}")
        data_partners_list.append(data_partner)
    
    print(f"Data partners list for version {staypoints_version}: {data_partners_list}")
    
    return data_partners_list


def folder_exists(path):
    s3_client = boto3.client('s3')
    bucket_name = path.split('/')[2]
    prefix = '/'.join(path.split('/')[3:])
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return 'Contents' in response

def read_staypoints(spark, run_date, country):
    ad_ex_list_v4 = fetch_data_partners_from_s3("v4")
    ad_ex_list_v6 = fetch_data_partners_from_s3("v6")

    path_list_v4 = []
    for data_partner in ad_ex_list_v4:
        for hour in range(24):
            staypoints_day_path = staypoints_path.format(
                "v4"
            ) + f"dataPartner={data_partner}/year={run_date.year:04d}/month={run_date.month:02d}/day={run_date.day:02d}/hour={hour:02d}/country={country}/"
            if folder_exists(staypoints_day_path):
                path_list_v4.append(staypoints_day_path)
            else:
                print(f"Folder does not exist: {staypoints_day_path}")

    path_list_v6 = []
    for data_partner in ad_ex_list_v6:
        for hour in range(24):
            staypoints_day_path = staypoints_path.format(
                "v6"
            ) + f"dataPartner={data_partner}/year={run_date.year:04d}/month={run_date.month:02d}/day={run_date.day:02d}/hour={hour:02d}/country={country}/"
            if folder_exists(staypoints_day_path):
                path_list_v6.append(staypoints_day_path)
            else:
                print(f"Folder does not exist: {staypoints_day_path}")

    day_staypoints_v4 = None
    if path_list_v4:
        day_staypoints_v4 = (
            spark.read.option("basePath", "s3://near-datamart/staypoints/version=v4/")
            .parquet(*path_list_v4)
            .select(F.lower("ifa").alias("ifa"), "aspkId", "geoHash9", "eventDTLocal")
            .withColumnRenamed("eventDTLocal", "run_date")
        )
        print(f"Read {day_staypoints_v4.count()} records from v4 for {run_date}")

    day_staypoints_v6 = None
    if path_list_v6:
        day_staypoints_v6 = (
            spark.read.option("basePath", "s3://near-datamart/staypoints/version=v6/")
            .parquet(*path_list_v6)
            .select(F.lower("ifa").alias("ifa"), "aspkId", "geoHash9", "eventDTLocal")
            .withColumnRenamed("eventDTLocal", "run_date")
        )
        print(f"Read {day_staypoints_v6.count()} records from v6 for {run_date}")

    if day_staypoints_v4 and day_staypoints_v6:
        day_staypoints = day_staypoints_v4.union(day_staypoints_v6)
        print(f"Combined v4 and v6 records count for {run_date}: {day_staypoints.count()}")
    elif day_staypoints_v4:
        day_staypoints = day_staypoints_v4
        print(f"Using only v4 records for {run_date}: {day_staypoints_v4.count()}")
    elif day_staypoints_v6:
        day_staypoints = day_staypoints_v6
        print(f"Using only v6 records for {run_date}: {day_staypoints_v6.count()}")
    else:
        print(f"No records found for {run_date}")
        return None

    return day_staypoints




def process_date_range(spark, start_date, end_date, country, places_df, gh_levels, final_output_path):
    current_date = start_date
    while current_date <= end_date:
        day_staypoints = read_staypoints(spark, current_date, country)
        if day_staypoints:
            for level in gh_levels:
                day_staypoints = day_staypoints.withColumn(f"gh{level}", F.expr(f"substring(geoHash9, 1, {level})"))
            
            mapped_footfalls = None
            for level in gh_levels:
                temp_join = day_staypoints.join(places_df, day_staypoints[f"gh{level}"] == places_df["geohash"], "inner")
                if mapped_footfalls is None:
                    mapped_footfalls = temp_join
                else:
                    mapped_footfalls = mapped_footfalls.union(temp_join)
            
            mapped_footfalls = mapped_footfalls.select("ifa", "aspkId", "geoHash9", "run_date", "near_poi_id").distinct()
            final_output_date_path = final_output_path + f"date={current_date.strftime('%Y-%m-%d')}/"
            mapped_footfalls.coalesce(10).write.option("compression", "snappy").parquet(final_output_date_path, mode="overwrite")
            print(f"Written data to: {final_output_date_path}")
        else:
            print(f"No data found for date: {current_date.strftime('%Y-%m-%d')}")
        
        current_date += timedelta(days=1)





if __name__ == "__main__":
    spark = SparkSession.builder.appName("FootfallsDailyOptimized").getOrCreate()
    start_date_str = "2023-01-14"
    end_date_str = "2024-01-14"
    country = "USA"
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")


    input_path = "s3://staging-near-data-analytics/shashwat/ps-3929/files/Mill Casino and Hotel_6595c63b5220ef2f7dce72e4.json"
    output_path = "s3://staging-near-data-analytics/shashwat/ps-3929/places_geohash/Texas/"
    fetch_places(spark, input_path, output_path)



    staypoints_bucket = "near-datamart"
    staypoints_path = "s3://near-datamart/staypoints/version={}/"
    places_geohash_path = "s3://staging-near-data-analytics/shashwat/ps-3929/places_geohash/Texas/"
    final_output_path = "s3://staging-near-data-analytics/shashwat/ps-3929/poi/final_footfall/"
    places_df = spark.read.parquet(places_geohash_path)
    places_df = places_df.withColumn("gh_level", F.length(F.col("geohash")))
    gh_levels = [row["gh_level"] for row in places_df.select("gh_level").distinct().collect()]
    process_date_range(spark, start_date, end_date, country, places_df, gh_levels, final_output_path)
