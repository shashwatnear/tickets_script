import geohash
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import math
from math import radians, sin, cos, asin, sqrt
import argparse
from datetime import datetime

spark = SparkSession.builder.appName("pollo tropical automation part1").getOrCreate()

# ################## UDF #####################

todays_date = datetime.today().strftime('%Y-%m-%d')

# ################## Functions #####################

# Read store visits data from S3
def read_store_visits(path):
    try:
        df = spark.read.parquet(path)
        df = df.withColumn("visited_date", expr("date_format(run_date, 'yyyy-MM-dd')")) \
               .withColumnRenamed("ifa", "visited_ifa")
        return df
    except Exception as e:
        raise Exception(f"Error reading store visits data: {e}")

# Read website visits data from S3
def read_website_visits(path):
    try:
        df = spark.read.parquet(path).select("unique_record_id", "ifa", "country")
        df = df.withColumn('impression_date', to_date(substring(col('unique_record_id'), 1, 10), 'yyyy-MM-dd'))
        return df
    except Exception as e:
        raise Exception(f"Error reading website visits data: {e}")

# Join data for store and website visits
def join_data(store_visits_df, website_visits_df, visit_start_dt, visit_end_dt, attr_window):
    try:
        joined_df = store_visits_df.join(website_visits_df, store_visits_df.visited_ifa == website_visits_df.ifa)
        joined_df = joined_df.withColumn("attribution", datediff(col("visited_date"), col("impression_date")))
        joined_df = joined_df.where((col("attribution") <= attr_window) & (col("attribution") >= 0))
        joined_df = joined_df.filter((col("visited_date") >= visit_start_dt) & (col("visited_date") <= visit_end_dt))
        return joined_df
    except Exception as e:
        raise Exception(f"Error processing DataFrames: {e}")

def write_output(df, path, ifa_output_path):
    try:
        # Check if the DataFrame is empty
        if df.rdd.isEmpty():
            empty_df = spark.createDataFrame([], df.schema)
            empty_df.write.mode("append").csv(path)
        else:
            # Write the full DataFrame to the provided path
            df.write.mode("append").csv(path)

            # Extract the 'ifa' column and save it to the specified path
            ifa_df = df.select("ifa").dropDuplicates()
            ifa_df.coalesce(1).write.mode("append").csv(ifa_output_path, header = True)

        return "SUCCESS"
    except Exception as e:
        raise Exception(f"Error writing output to S3: {e}")

# Main function for handling attribution visits
def getAttrVisit(visit_start_dt, visit_end_dt, todays_date, attr_window):
    store_visits_path = "s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=669a976dfb552b2cc7bca3e6/data_cache/visits/location=b46bed1a3fb1d389723148d557b538d1/*/*"
    website_visits_path = "s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=669a976dfb552b2cc7bca3e6/data_cache/impressions/*/*"
    exposed_ids_path = f"s3://staging-near-data-analytics/shashwat/pollo_tropical/{todays_date}/all_ids/{visit_start_dt}_{visit_end_dt}"
    ifa_output_path = f"s3://staging-near-data-analytics/shashwat/pollo_tropical/{todays_date}/all_ids_combined/"

    store_visits_df = read_store_visits(store_visits_path)
    website_visits_df = read_website_visits(website_visits_path)
    joined_df = join_data(store_visits_df, website_visits_df, visit_start_dt, visit_end_dt, attr_window)
    
    return write_output(joined_df, exposed_ids_path, ifa_output_path)

# Function to get IFA data
def get_ifas(todays_date):
    df1 = spark.read.parquet('s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=669a976dfb552b2cc7bca3e6/data_cache/visits/location=b46bed1a3fb1d389723148d557b538d1/*')
    df2 = spark.read.csv(f's3://staging-near-data-analytics/shashwat/pollo_tropical/{todays_date}/all_ids_combined/*', header = True)
    df3 = df1.join(df2, on = 'ifa', how = 'inner')
    df3.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/pollo_tropical/{todays_date}/inner_join_result/")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Enter date format like YYYY-MM-DD')
    parser.add_argument('--start_date', type=str, help='Start date in YYYY-MM-DD format', required=True)
    parser.add_argument('--end_date', type=str, help='End date in YYYY-MM-DD format', required=True)
    parser.add_argument('--attr_window', type=int, help='1 or 2 or 3', required=True)

    args = parser.parse_args()
    start = args.start_date
    end = args.end_date
    attr_window = args.attr_window

    if getAttrVisit(start, end, todays_date, attr_window) == "SUCCESS":
        print("Attributed visits processing complete.")
    
    get_ifas(todays_date)

# spark-submit --deploy-mode cluster --executor-cores 2 --num-executors 8 --executor-memory 10g --driver-memory 4g --driver-cores 2 --py-files /home/hadoop/common/util.py --conf spark.dynamicAllocation.enabled=false s3://staging-near-data-analytics/shashwat/pollo_tropical/scripts/automation1.py --start_date "2024-08-01" --end_date "2024-08-05" --attr_window 2
