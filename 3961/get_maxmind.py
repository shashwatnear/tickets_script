from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import maxminddb

# Configure SparkSession (assuming you have Spark installed)
spark = SparkSession.builder \
    .appName("3976 IP Maxmind Processing") \
    .getOrCreate()

def get_city(ip):
    reader = maxminddb.open_database("/Users/nlblr245/Downloads/GeoIP2-City.mmdb")
    data = reader.get(ip)
    reader.close()
    if data and 'city' in data and 'names' in data['city'] and 'en' in data['city']['names']:
        return data['city']['names']['en']
    else:
        return None
fetch_city = udf(get_city, StringType())

# Read the CSV file using Spark
df = spark.read.csv("/Users/nlblr245/Desktop/filtered_ips.csv", header=True)

# Create the final DataFrame with IP and MaxMind value (using UDF)
df = df.withColumn("city", fetch_city(col("ip")))

# Write the final DataFrame to CSV
df.coalesce(1).write.option("header", True).mode('append').csv("/Users/nlblr245/Desktop/tickets/maxmind/ip_country_city_postcode/")

# Stop the SparkSession (optional, but good practice)
spark.stop()

# reader = maxminddb.open_database("/Users/nlblr245/Downloads/GeoIP2-City.mmdb")
# data = reader.get("/Users/nlblr245/Desktop/filtered_ips.csv")
# print(data)
# spark.stop()