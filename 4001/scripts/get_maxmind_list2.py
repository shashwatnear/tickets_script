from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, IntegerType
import maxminddb

# Configure SparkSession
spark = SparkSession.builder \
    .appName("3976 - IP Maxmind Processing") \
    .getOrCreate()

# Define UDFs to extract relevant fields
def get_maxmind_data(ip):
    reader = maxminddb.open_database("/Users/nlblr245/Downloads/GeoIP2-City.mmdb")
    data = reader.get(ip)
    reader.close()
    if data:
        city = data.get('city', {})
        continent = data.get('continent', {})
        country = data.get('country', {})
        location = data.get('location', {})
        postal = data.get('postal', {})
        subdivision = data.get('subdivisions', [{}])[0] if 'subdivisions' in data and len(data['subdivisions']) > 0 else {}
        registered_country = data.get('registered_country', {})

        return (
            city.get('geoname_id', None),
            city.get('names', {}).get('en', None),
            continent.get('geoname_id', None),
            continent.get('code', None),
            continent.get('names', {}).get('en', None),
            country.get('geoname_id', None),
            country.get('iso_code', None),
            country.get('names', {}).get('en', None),
            location.get('latitude', None),
            location.get('longitude', None),
            location.get('accuracy_radius', None),
            location.get('time_zone', None),
            postal.get('code', None),
            subdivision.get('geoname_id', None),
            subdivision.get('iso_code', None),
            subdivision.get('names', {}).get('en', None),
            registered_country.get('geoname_id', None),
            registered_country.get('iso_code', None),
            registered_country.get('names', {}).get('en', None)
        )
    return (None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)

# Register UDF
schema = StructType([
    StructField("city_geoname_id", IntegerType(), True),
    StructField("city_name", StringType(), True),
    StructField("continent_geoname_id", IntegerType(), True),
    StructField("continent_code", StringType(), True),
    StructField("continent_name", StringType(), True),
    StructField("country_geoname_id", IntegerType(), True),
    StructField("country_iso_code", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("accuracy_radius", IntegerType(), True),
    StructField("time_zone", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("subdivision_geoname_id", IntegerType(), True),
    StructField("subdivision_iso_code", StringType(), True),
    StructField("subdivision_name", StringType(), True),
    StructField("registered_country_geoname_id", IntegerType(), True),
    StructField("registered_country_iso_code", StringType(), True),
    StructField("registered_country_name", StringType(), True)
])

fetch_maxmind_data = udf(get_maxmind_data, schema)

# Read the Parquet file using Spark
df = spark.read.csv("/Users/nlblr245/Desktop/ticket/4001/iplistsharedbyme/list2.csv", header = True)

# Create the final DataFrame with IP and MaxMind values
df = df.withColumn("maxmind_data", fetch_maxmind_data(col("ip")))

# Flatten the struct columns
df = df.select(
    col("ip"),
    col("maxmind_data.city_geoname_id"),
    col("maxmind_data.city_name"),
    col("maxmind_data.continent_geoname_id"),
    col("maxmind_data.continent_code"),
    col("maxmind_data.continent_name"),
    col("maxmind_data.country_geoname_id"),
    col("maxmind_data.country_iso_code"),
    col("maxmind_data.country_name"),
    col("maxmind_data.latitude"),
    col("maxmind_data.longitude"),
    col("maxmind_data.accuracy_radius"),
    col("maxmind_data.time_zone"),
    col("maxmind_data.postal_code"),
    col("maxmind_data.subdivision_geoname_id"),
    col("maxmind_data.subdivision_iso_code"),
    col("maxmind_data.subdivision_name"),
    col("maxmind_data.registered_country_geoname_id"),
    col("maxmind_data.registered_country_iso_code"),
    col("maxmind_data.registered_country_name")
)

# Show the resulting DataFrame
df.show(truncate=False)

# Write the final DataFrame to CSV
df.coalesce(1).write.option("header", True).mode('append').csv("/Users/nlblr245/Desktop/tickets/4001/list2/")

# Stop the Spark session
spark.stop()
