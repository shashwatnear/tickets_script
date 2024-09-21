from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.sql import SparkSession
import requests,json
import proximityhash
import Geohash
import pandas as pd
import numpy as np
import time
import subprocess
from util import *


spark = SparkSession.builder.appName("Tourist cards for NewZealand").getOrCreate()
sc = spark.sparkContext

# # for NZL we took 30 days of data
# import datetime
# currentdate = datetime.datetime.now().strftime("%Y-%m-%d")
# print(currentdate)

# start = "2024-06-01"
# end = "2024-06-30"
# dates = get_dates_between(start, end)

# countries = ['NZL']
# for country in countries:
#     for date in dates:
#         try:
#             path = "s3://near-rule-engine-data/rule-input/{}/{}".format(country,date)
#             data = spark.read.parquet(path)
#             data_footfall = data.select(["aspkId",'v_date']).dropDuplicates()
#             data_footfall.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3948/nzl/rule_engg/{}/{}/".format(country,date))
#         except Exception as e:
#             print(e)

# ##############################
            
# start = "2024-04-01"
# end = "2024-06-30"
# dates = get_dates_between(start, end)

# countries = ['JPN','MYS', 'NLD','PHL', 'SAU', 'SGP', 'THA', 'TUR','VNM','IND','IDN','ARE','IRL','DEU','FRA','GBR','ESP','HKG','ITA','AUS','TWN','CAN','MEX']
# for country in countries:
#     for date in dates:
#         try:
#             path = "s3://near-rule-engine-data/rule-input/{}/{}".format(country,date)
#             data = spark.read.parquet(path)
#             data_footfall = data.select(["aspkId",'v_date']).dropDuplicates()
#             data_footfall.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3948/nzl/rule_engg/{}/{}/".format(country,date))
#         except Exception as e:
#             print(e)

            
# ##############################


# schema1 = StructType([StructField("aspkId", StringType(), False),
#                       StructField("HomeLocation", StringType(), True),
#                       StructField("new_days", StringType(),True) ])

# df = spark.createDataFrame(data = sc.emptyRDD(), schema=schema1)

# nzl = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3948/nzl/rule_engg/NZL/*/*").dropDuplicates()
# nzl = nzl.withColumnRenamed('v_date','nzl_date')

# nzl_cnts = nzl.groupBy('aspkId').agg(countDistinct('nzl_date').alias('nzl_days'))


# # countries1 = ['MYS', 'NLD', 'JPN', 'PHL', 'SAU', 'SGP', 'THA', 'TUR','VNM','IND','IDN','ARE','IRL','DEU','FRA','GBR','ESP','HKG','ITA','AUS','TWN','CAN','MEX']
# countries1 = ['USA']
# for country in countries1:
#     # new = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3948/nzl/rule_engg/{}/*/*".format(country)).dropDuplicates()
#     new = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3948/aus/rule_engg/{}/*/*".format(country)).dropDuplicates()
#     new = new.withColumnRenamed('v_date','{}_date'.format(country))
 
#     new_cnts = new.groupBy('aspkId').agg(countDistinct('{}_date'.format(country)).alias('{}_days'.format(country)))

#     data = nzl_cnts.join(new_cnts, on='aspkId', how='inner').dropDuplicates()

#     data1 = data.filter( (col('nzl_days')>1) & (col('{}_days'.format(country))>1))
#     data1 = data1.withColumn('HomeLocation', when(col('nzl_days') >= col('{}_days'.format(country)), "NewZealand").otherwise("{}".format(country)))
#     data1 = data1.withColumn('new_days', (col('{}_days'.format(country)) - col('nzl_days'))).select('aspkId','HomeLocation','new_days').dropDuplicates()
#     data1.write.mode('append').parquet("s3://staging-near-data-analytics/shashwat/ps-3948/nzl/Tourist_card/{}/Tourist_90_data/{}".format(currentdate, country))
    

# df = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3948/nzl/Tourist_card/{}/Tourist_90_data/*/*".format(currentdate))
# df1 = df.filter(col('HomeLocation')!="NewZealand")

# df1.write.mode('append').parquet("s3://staging-near-data-analytics/shashwat/ps-3948/nzl/all_days/")

# ##############################

# partition by day differenc

# df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3948/nzl/all_days/*')
# days_diff = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89]
# countries = ['ARE', 'AUS', 'CAN', 'DEU', 'ESP', 'FRA', 'GBR', 'HKG', 'IDN', 'IND', 'IRL', 'ITA', 'JPN', 'MEX', 'MYS', 'NLD', 'PHL', 'SAU', 'SGP', 'THA', 'TUR', 'TWN', 'USA', 'VNM']

# for country in countries:
#     result = None
#     for day in days_diff:
#         temp = df.filter(col('new_days') >= day).withColumnRenamed('HomeLocation', 'country').filter((col('country') == country))
#         temp = temp.withColumn('day_diff', lit(day))
#         temp = temp.groupBy('country', 'day_diff').agg(countDistinct('aspkId').alias('id_count')).orderBy('day_diff')
#         if result is None:
#             result = temp
#         else:
#             result = result.union(temp)
#     result.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-3948/nzl/days_diff_by_country/{country}/", header = True)


# ##############################
import subprocess
import json
import boto3

countries = ['ARE', 'AUS', 'CAN', 'DEU', 'ESP', 'FRA', 'GBR', 'HKG', 'IDN', 'IND', 'IRL', 'ITA', 'JPN', 'MEX', 'MYS', 'NLD', 'PHL', 'SAU', 'SGP', 'THA', 'TUR', 'TWN', 'USA', 'VNM']

# Base URL and API key
url = "https://int-apigw.adnear.net/allspark-api-v4/internal_apis/v1/custom_people_segment_combo"
apikey = "967a79c7-9768-4b38-88c8-035eedef67d6"

# S3 base path
s3_base_path = "s3://staging-near-data-analytics/shashwat/ps-3948/nzl/aspkid_to_create_cards"

# Initialize S3 client
s3 = boto3.client('s3')

def get_s3_file_path(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.csv'):
            return f"s3://{bucket}/{obj['Key']}"
    return None

# Function to run the curl command
def run_curl_command(country, s3_file_path):
    data = {
        "settings": {"visit_period": 90},
        "insights": True,
        "name": f"{country}-Inbound",
        "country": "NZL",  # Assuming 'NZL' is a static value as per your example
        "aspkids_file": s3_file_path
    }
    headers = {"Content-type": "application/json"}
    
    # Convert data to JSON string
    data_str = json.dumps(data)
    
    # Construct curl command
    command = [
        "curl",
        "-XPOST",
        "--header", f"Content-type: application/json",
        "--data", data_str,
        f"{url}?apikey={apikey}"
    ]
    
    # Run the curl command
    result = subprocess.run(command, capture_output=True, text=True)
    
    # Check the result
    if result.returncode == 0:
        print(f"Successfully executed for country {country}")
        print("Response:", result.stdout)
    else:
        print(f"Failed to execute for country {country}")
        print("Error:", result.stderr)

# Extract bucket name from s3_base_path
s3_bucket = s3_base_path.split('/')[2]
s3_prefix_base = '/'.join(s3_base_path.split('/')[3:])

# Loop through each country and run the curl command
for country in countries:
    s3_prefix = f"{s3_prefix_base}/{country}/"
    s3_file_path = get_s3_file_path(s3_bucket, s3_prefix)
    if s3_file_path:
        run_curl_command(country, s3_file_path)
    else:
        print(f"No CSV file found for country {country} in path {s3_prefix}")

# ##############################
        
# curl -XPOST --header "Content-type: application/json" --data '{"settings":{"visit_period":90},"insights":true, "name":"ARE-Inbound", "country":"NZL", "file": "s3://staging-near-data-analytics/shashwat/ps-3948/nzl/aspkid_to_create_cards/ARE/part-00000-8edc1cfc-a7c7-4e8c-acb0-d003affb8015-c000.csv"}' https://int-apigw.adnear.net/allspark-api-v4/internal_apis/v1/custom_people_segment_combo?apikey=967a79c7-9768-4b38-88c8-035eedef67d6