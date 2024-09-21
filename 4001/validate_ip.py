from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import re

# Configure SparkSession
spark = SparkSession.builder \
    .appName("IP Cleanup") \
    .getOrCreate()

# Regular expression for a valid IPv4 address
ipv4_pattern = re.compile(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b')

# UDF to extract the first valid IPv4 address
def extract_first_valid_ip(ip_list):
    ips = ip_list.split(',')
    for ip in ips:
        ip = ip.strip()
        if ipv4_pattern.match(ip):
            return ip
    return None

extract_first_valid_ip_udf = udf(extract_first_valid_ip, StringType())

# DataFrame
df = spark.read.csv("/Users/nlblr245/Desktop/ticket/4001/iplistsharedbyme/list3.csv", header = True)

# Apply the UDF to clean the IPs
df_cleaned = df.withColumn("ip", extract_first_valid_ip_udf(col("ip")))

# Show the cleaned DataFrame
df_cleaned.show(truncate=False)
df_cleaned.coalesce(1).write.option("header", True).mode('append').csv("/Users/nlblr245/Desktop/tickets/4001/iplistsharedbyme/")

# Stop the Spark session
spark.stop()
