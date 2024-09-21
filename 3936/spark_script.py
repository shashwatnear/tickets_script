from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
import maxminddb

# Configure SparkSession (assuming you have Spark installed)
spark = SparkSession.builder \
    .appName("IP Maxmind Processing") \
    .getOrCreate()

# Define a UDF to handle potential MaxMind errors and missing IPs
# @udf(returnType=lit(""))
def get_maxmind_value(ip):
    try:
        reader = maxminddb.open_database("s3://staging-near-data-analytics/shashwat/ps-3936/files/GeoIP2-City.mmdb")
        return reader.get(ip)
    except (maxminddb.errors.AddressNotFoundError, FileNotFoundError):
        return lit(None)  # Handle missing IP or database errors gracefully

fetch_maxmind = udf(get_maxmind_value)

# Read the CSV file using Spark
df = spark.read.csv("s3://staging-near-data-analytics/shashwat/ps-3936/ip_list/output.csv", header=True)

# Create the final DataFrame with IP and MaxMind value (using UDF)
dfg = df.withColumn("maxmind_value", fetch_maxmind(col("ip")))

# Write the final DataFrame to JSON (lines=True for human readability)
dfg.write.json("s3://staging-near-data-analytics/shashwat/ps-3936/maxmind_data/", mode="overwrite")

# Stop the SparkSession (optional, but good practice)
spark.stop()