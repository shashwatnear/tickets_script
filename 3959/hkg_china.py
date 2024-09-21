#Script for 3156 ticket
from pyspark.sql.functions import *
from datetime import date,timedelta,datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import datetime
from datetime import datetime, timedelta
from pyspark.sql.window import Window
import requests,json
import pandas as pd
import proximityhash
import Geohash
import pyspark.sql.functions as f
import pytz
from calendar import monthrange
from pyspark.sql.functions import col
import subprocess


spark = SparkSession.builder.appName("HKG_China_tourists").getOrCreate()
sc = spark.sparkContext

############ HKG Refined Extraction ###################

import datetime
currentdate = datetime.datetime.now().strftime("%Y-%m-%d")
print(currentdate)

from datetime import datetime
datelist = pd.date_range(end=datetime.today(), periods=65).tolist()

dates = []
for i in datelist:
    res = str(i.date())
    dates.append(res)

for date in dates[-65:]:
	try:
		date_obj = datetime.strptime(date, "%Y-%m-%d")
		day = "{:02d}".format(date_obj.day)
		month = '{:02d}'.format(date_obj.month)
		year = date_obj.year 
		path = "s3://near-data-warehouse/refined/dataPartner=*/year={}/month={}/day={}/hour=*/country=HKG/*".format(year,month,day)
		print(path)
		data = spark.read.parquet(path)
		data_footfall = data.select(['aspkId','devLanguage','eventTs','devCarrier','eventDTLocal']).dropDuplicates()
		data_footfall.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/TouristCardsData/{currentdate}/HKG/refined/{date}/")
	except Exception as e:
		print(e)

# # #######################################################


#For previous month day 1 and day 31 datetime
current_date = datetime.now()
current_year = current_date.year
Previous_month = current_date - timedelta(days=30)
Previous_month = Previous_month.strftime("%m")

one_months_ago = current_date - timedelta(days=30)

year = one_months_ago.year
month = one_months_ago.month

num_days = monthrange(year, month)[1]

first_day = datetime(year, month, 1)
last_day = datetime(year, month, num_days)

first_day_timestamp = str(first_day)
last_day_timestamp = str(last_day)

#For before previous month day 1 and day 31 datetime
two_months_ago = current_date - timedelta(days=60)

year_2 = two_months_ago.year
month_2 = two_months_ago.month

num_days_2 = monthrange(year_2, month_2)[1]

first_day_2 = datetime(year_2, month_2, 1)
last_day_2 = datetime(year_2, month_2, num_days_2)

first_day_timestamp_2 = str(first_day_2)
last_day_timestamp_2 = str(last_day_2)

import datetime
currentdate = datetime.datetime.now().strftime("%Y-%m-%d")
print(currentdate)

# ##########################################################################
total = spark.read.parquet(f"s3://staging-near-data-analytics/shashwat/TouristCardsData/{currentdate}/HKG/refined/*/*")
oct_data = total.select('aspkId','eventTs','devLanguage').where(col('eventDTLocal').between(first_day_timestamp,last_day_timestamp)) #
sep = total.select('aspkId').where(col('eventDTLocal').between(first_day_timestamp_2,last_day_timestamp_2))

homeloc = spark.read.parquet("s3://near-datamart/homeLocation/version=v1/MasterDB/dataPartner=combined/year={}/month={}/day=*/country=HKG/*".format(current_year,Previous_month))
homeloc = homeloc.select(['aspkId','geoHash8']).dropDuplicates()
homeloc = homeloc.withColumnRenamed('geoHash8','homelocation')

mapped = oct_data.join(homeloc,on='aspkId').dropDuplicates()
mapped.write.mode('append').parquet(f's3://staging-near-data-analytics/shashwat/TouristCardsData/{currentdate}/HKG/chinese_tourist/')


mapped = spark.read.parquet(f's3://staging-near-data-analytics/shashwat/TouristCardsData/{currentdate}/HKG/chinese_tourist/*')
print("devices with HLs in HKG")
print(mapped.select('aspkId').distinct().count()) #


### taking devices which are not having HLs in HKG

hkg_hl = spark.read.parquet(f's3://staging-near-data-analytics/shashwat/TouristCardsData/{currentdate}/HKG/chinese_tourist/*').select(['aspkId']).dropDuplicates()

# print(hkg_hl.select('aspkId').distinct().count()) #

not_hkg = oct_data.join(hkg_hl,on='aspkId',how='left_anti').dropDuplicates()
not_hkg.write.mode('append').parquet(f's3://staging-near-data-analytics/shashwat/TouristCardsData/{currentdate}/HKG/HL_not_in_HKG/')

not_hkg = spark.read.parquet(f's3://staging-near-data-analytics/shashwat/TouristCardsData/{currentdate}/HKG/HL_not_in_HKG/*')
print("devices not having HLs in HKG")
print(not_hkg.select('aspkId').distinct().count()) #


#### datediff logic for devices with no HL in HKG
not_hkg = spark.read.parquet(f's3://staging-near-data-analytics/shashwat/TouristCardsData/{currentdate}/HKG/HL_not_in_HKG/*')
data = not_hkg.withColumn('timestamp',from_unixtime("eventTs", "yyyy-MM-dd HH:mm:ss"))
data1 = data.withColumn('date',data.timestamp.substr(1,10))
ft = data1.groupBy('aspkId').agg(datediff(max('date'),min('date')).alias('date_diff'))


sdt = ft.filter((col('date_diff')>=1)&(col('date_diff')<10))
print("devices count between freq 1 to 9 ")
print(sdt.select('aspkId').distinct().count()) #
sdt_ch = sdt.join(data1,on='aspkId').dropDuplicates()
sdt_ch1 = sdt_ch.filter(col('devLanguage')=='zh')
print("devices count between freq 1 to 9 and device lang as china")
print(sdt_ch1.select('aspkId').distinct().count()) #


sdt_ch1_cnt = sdt_ch1.join(sep,on='aspkId',how='left_anti').dropDuplicates()
sdt_ch1_cnt.write.mode('append').parquet(f's3://staging-near-data-analytics/shashwat/TouristCardsData/{currentdate}/HKG/CHN_Tourists_in_HKG/')

sdt_ch1_cnt =spark.read.parquet(f's3://staging-near-data-analytics/shashwat/TouristCardsData/{currentdate}/HKG/CHN_Tourists_in_HKG/*')
print("devices count between freq 1 to 9 and device lang as china and not seen in mar22")
print(sdt_ch1_cnt.select('aspkId').distinct().count())


df = spark.read.parquet(f's3://staging-near-data-analytics/shashwat/TouristCardsData/{currentdate}/HKG/CHN_Tourists_in_HKG/*')
print(df.select('aspkId').distinct().count())
final = df.select('aspkId').distinct()
final.coalesce(1).write.option("header",True).mode('append').csv(f's3://staging-near-data-analytics/shashwat/ps-3959/hkg/aspkid_to_create_cards/CHN/')

# ######################

import subprocess
import json
import boto3

# List of countries
countries = ['CHN']

# Base URL and API key
url = "https://int-apigw.adnear.net/allspark-api-v4/internal_apis/v1/custom_people_segment_combo"
apikey = "6b4912ed-9063-4d20-9886-755ee95f95b7"

# S3 base path
s3_base_path = "s3://staging-near-data-analytics/shashwat/ps-3959/hkg/aspkid_to_create_cards"

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
        "settings": {"visit_period": 30},
        "insights": True,
        "name": f"{country}-Inbound",
        "country": "HKG",  # Assuming 'HKG' is a static value as per your example
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