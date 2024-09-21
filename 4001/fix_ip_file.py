from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col

# Create a Spark session
spark = SparkSession.builder.appName("IP Processing").getOrCreate()

# Load the CSV file into a DataFrame
input_file = "/Users/nlblr245/Desktop/ticket/4001/iplistsharedbyme/list3_clean.csv"
df = spark.read.csv(input_file, header=True)

# Split the IPs based on comma and explode the array to get one IP per row
df_split = df.withColumn("ip", explode(split(col("ip"), ",")))

# Save the result to a CSV file
output_file = "/Users/nlblr245/Desktop/tickets/4001/iplistsharedbyme_new1/"
df_split.select("ip").write.csv(output_file, header=True)

# Show the result (optional)
df_split.show(truncate=False)