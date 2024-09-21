from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import maxminddb

# Configure SparkSession (assuming you have Spark installed)
spark = SparkSession.builder \
    .appName("3976 - IP Maxmind Processing") \
    .getOrCreate()

def get_state(ip):
    reader = maxminddb.open_database("/Users/nlblr245/Downloads/GeoIP2-City.mmdb")
    data = reader.get(ip)
    reader.close()
    if data and 'subdivisions' in data and len(data['subdivisions']) > 0:
        subdivision = data['subdivisions'][0]
        if 'names' in subdivision and 'en' in subdivision['names']:
            return subdivision['names']['en']
    return None
fetch_state = udf(get_state, StringType())

def get_pc(ip):
    reader = maxminddb.open_database("/Users/nlblr245/Downloads/GeoIP2-City.mmdb")
    data = reader.get(ip)
    reader.close()
    if data and 'postal' in data and 'code' in data['postal']:
        return data['postal']['code']
    else:
        return None
fetch_pc = udf(get_pc, StringType())

# Read the CSV file using Spark
df = spark.read.parquet("/Users/nlblr245/Desktop/ticket/3976/ip_ifa/*/*")

# Create the final DataFrame with IP and MaxMind value (using UDF)
df = df.withColumn("state", fetch_state(col("ip")))
df = df.withColumn("postal_code", fetch_pc(col('ip')))
df.show()

# Write the final DataFrame to CSV
df.coalesce(1).write.option("header", True).mode('append').csv("/Users/nlblr245/Desktop/tickets/3976/postcode_ip_ifa_state/")

spark.stop()

# reader = maxminddb.open_database("/Users/nlblr245/Downloads/GeoIP2-City.mmdb")
# data = reader.get("/Users/nlblr245/Desktop/filtered_ips.csv")
# print(data)
# spark.stop()