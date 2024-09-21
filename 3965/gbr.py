from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.sql import SparkSession
import requests,json
import proximityhash
import Geohash
import time
import subprocess
from util import *
import boto3
import pandas as pd
import datetime
from datetime import datetime,timedelta

spark = SparkSession.builder.appName("Tourist cards for Malaysia").getOrCreate()
sc = spark.sparkContext

# for SGP we took 30 days of data
currentdate = datetime.now().strftime("%Y-%m-%d")
print(currentdate)

end_date = datetime.today() - timedelta(days=10)
start_date = end_date - timedelta(days=30)
datelist = pd.date_range(start=start_date, end=end_date).tolist()

dates = []
for i in datelist:
    res = str(i.date())
    dates.append(res)

countries = ['GBR']

for country in countries:
    for date in dates:
        try:
            path = "s3://near-rule-engine-data/rule-input/{}/{}".format(country,date)
            data = spark.read.parquet(path)
            data_footfall = data.select(["aspkId",'v_date']).dropDuplicates()
            data_footfall.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3965/gbr/rule_engg/{}/{}/".format(country,date))
        except Exception as e:
            print(e)

# # ###############################

end_date = datetime.today() - timedelta(days=10)
start_date = end_date - timedelta(days=90)
datelist = pd.date_range(start=start_date, end=end_date).tolist()

dates = []
for i in datelist:
    res = str(i.date())
    dates.append(res)

countries = ['ARE', 'AUS', 'CAN', 'CHE', 'DEU', 'ESP', 'FRA', 'HKG', 'IDN', 'IND', 'IRL', 'ITA', 'JPN', 'MEX', 'MYS', 'NLD', 'NOR', 'NZL', 'PHL', 'SAU', 'SGP', 'THA', 'TUR', 'TWN', 'USA', 'VNM']

for country in countries:
    for date in dates:
        try:
            path = "s3://near-rule-engine-data/rule-input/{}/{}".format(country,date)
            data = spark.read.parquet(path)
            data_footfall = data.select(["aspkId",'v_date']).dropDuplicates()
            data_footfall.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3965/gbr/rule_engg/{}/{}/".format(country,date))
        except Exception as e:
            print(e)


###############################

gbr = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3965/gbr/rule_engg/GBR/*/*").dropDuplicates()
gbr = gbr.withColumnRenamed('v_date','gbr_date')

gbr_cnts = gbr.groupBy('aspkId').agg(countDistinct('gbr_date').alias('gbr_days'))

countries1 = ['ARE', 'AUS', 'CAN', 'CHE', 'DEU', 'ESP', 'FRA', 'HKG', 'IDN', 'IND', 'IRL', 'ITA', 'JPN', 'MEX', 'MYS', 'NLD', 'NOR', 'NZL', 'PHL', 'SAU', 'SGP', 'THA', 'TUR', 'TWN', 'USA', 'VNM']

for country in countries1:
    new = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3965/gbr/rule_engg/{}/*/*".format(country)).dropDuplicates()
    new = new.withColumnRenamed('v_date','{}_date'.format(country))
 
    new_cnts = new.groupBy('aspkId').agg(countDistinct('{}_date'.format(country)).alias('{}_days'.format(country)))

    data = gbr_cnts.join(new_cnts, on='aspkId', how='inner').dropDuplicates()

    data1 = data.filter( (col('gbr_days')>1) & (col('{}_days'.format(country))>1))
    data1 = data1.withColumn('HomeLocation', when(col('gbr_days') >= col('{}_days'.format(country)), "GreatBritain").otherwise("{}".format(country)))
    data1 = data1.withColumn('new_days', (col('{}_days'.format(country)) - col('gbr_days'))).select('aspkId','HomeLocation','new_days').dropDuplicates()
    data1.write.mode('append').parquet("s3://staging-near-data-analytics/shashwat/ps-3965/gbr/Tourist_card/{}/Tourist_90_data/{}".format(currentdate, country))

###############################

df = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3965/gbr/Tourist_card/{}/Tourist_90_data/*/*".format(currentdate))
df1 = df.filter(col('HomeLocation') != "GreatBritain")

df_1 = df1.filter(col('new_days') >= 1).withColumnRenamed('HomeLocation','country').filter((col('country').isin(['ARE', 'AUS', 'CAN', 'CHE', 'DEU', 'ESP', 'FRA', 'HKG', 'IDN', 'IND', 'IRL', 'ITA', 'JPN', 'MEX', 'MYS', 'NLD', 'NOR', 'NZL', 'PHL', 'SAU', 'SGP', 'THA', 'TUR', 'TWN', 'USA', 'VNM'])))

countries = ['ARE', 'AUS', 'CAN', 'CHE', 'DEU', 'ESP', 'FRA', 'HKG', 'IDN', 'IND', 'IRL', 'ITA', 'JPN', 'MEX', 'MYS', 'NLD', 'NOR', 'NZL', 'PHL', 'SAU', 'SGP', 'THA', 'TUR', 'TWN', 'USA', 'VNM']

for country in countries:
    temp = df_1.filter(col('country') == country).select('aspkId')
    temp.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-3965/gbr/aspkid_to_create_cards/{country}/", header = True)

# ###############################
    
# df = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3965/gbr/Tourist_card/{}/Tourist_90_data/*/*".format(currentdate))
# df = df.filter(col('HomeLocation') != "GreatBritain")

# days_diff = list(range(1, 90))
# countries = ['ARE', 'AUS', 'CAN', 'CHE', 'DEU', 'ESP', 'FRA', 'HKG', 'IDN', 'IND', 'IRL', 'ITA', 'JPN', 'MEX', 'MYS', 'NLD', 'NOR', 'NZL', 'PHL', 'SAU', 'SGP', 'THA', 'TUR', 'TWN', 'USA', 'VNM']

# result = None

# for day in days_diff:
#     temp = df.filter(col('new_days') >= day).withColumnRenamed('HomeLocation', 'country')
#     temp = temp.withColumn('day_diff', lit(day))
#     temp = temp.groupBy('country', 'day_diff').agg(countDistinct('aspkId').alias('id_count')).orderBy('day_diff')
    
#     if result is None:
#         result = temp
#     else:
#         result = result.union(temp)

# all_combinations = spark.createDataFrame([Row(day_diff=day, country=country) for day in days_diff for country in countries])
# result = all_combinations.join(result, on=['country', 'day_diff'], how='left').fillna(0)
# pivoted_result = result.groupBy('day_diff').pivot('country').sum('id_count').orderBy('day_diff')
# pivoted_result.coalesce(1).write.mode('append').csv("s3://staging-near-data-analytics/shashwat/ps-3965/gbr/days_diff_by_country/", header=True)

# ###############################

import subprocess
import json
import boto3

# List of countries
countries = ['ARE', 'AUS', 'CAN', 'CHE', 'DEU', 'ESP', 'FRA', 'HKG', 'IDN', 'IND', 'IRL', 'ITA', 'JPN', 'MEX', 'MYS', 'NLD', 'NOR', 'NZL', 'PHL', 'SAU', 'SGP', 'THA', 'TUR', 'TWN', 'USA', 'VNM']

# Base URL and API key
url = "https://int-apigw.adnear.net/allspark-api-v4/internal_apis/v1/custom_people_segment_combo"
apikey = "9e096c3b-0ff9-40b3-87e0-06b4374b7414"

# S3 base path
s3_base_path = "s3://staging-near-data-analytics/shashwat/ps-3965/gbr/aspkid_to_create_cards"

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
        "country": "GBR",  # Assuming 'HKG' is a static value as per your example
        "aspkids_file": s3_file_path
    }
    headers = {"Content-type": "application/json"}
    
    # Convert data to JSON string
    data_str = json.dumps(data)
    
    # Construct curl command
    command = [
        "curl",
        "-H", "Content-Type: application/json",
        "-X", "POST",
        "-d", data_str,
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

# ####################

# curl -XPOST --header "Content-type: application/json" --data '{"settings":{"visit_period":90},"insights":true, "name":"ESP Inbound", "country":"USA", "aspkids_file": ""}' https://int-apigw.adnear.net/allspark-api-v4/internal_apis/v1/custom_people_segment_combo?apikey=e4fec7a2-40fc-47b5-83a3-31156fa5be02