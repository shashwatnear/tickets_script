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


spark = SparkSession.builder.appName("test").getOrCreate()
sc = spark.sparkContext
'''
import datetime
currentdate = datetime.datetime.now().strftime("%Y-%m-%d")
#print(currentdate)

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
        path = "s3://near-data-warehouse/refined/dataPartner=*/year={}/month={}/day={}/hour=*/country=JPN/*".format(year,month,day)
        print(path)
        data = spark.read.parquet(path)
        data_footfall = data.select(['aspkId','devLanguage','eventTs','devCarrier','eventDTLocal']).dropDuplicates()
        data_footfall.write.mode("append").parquet("s3://staging-near-data-analytics/Adithya/JPN_Tourists_china_korea/{}/JPN_refined_data/{}/".format(currentdate,date))
    except Exception as e:
        print(e)
'''

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

# import datetime
# currentdate = datetime.datetime.now().strftime("%Y-%m-%d")
# print(currentdate)
currentdate = "2024-07-11"
##########################################################################

total = spark.read.parquet('s3://staging-near-data-analytics/Adithya/JPN_Tourists_china_korea/{}/JPN_refined_data/*/*'.format(currentdate))
oct_data = total.select('aspkId','eventTs','devLanguage').where(col('eventDTLocal').between(first_day_timestamp,last_day_timestamp)) #
sep = total.select('aspkId').where(col('eventDTLocal').between(first_day_timestamp_2,last_day_timestamp_2))


homeloc = spark.read.parquet("s3://near-datamart/homeLocation/version=v1/MasterDB/dataPartner=combined/year={}/month={}/day=*/country=JPN/*".format(current_year,Previous_month))
homeloc = homeloc.select(['aspkId','geoHash8']).dropDuplicates()
homeloc = homeloc.withColumnRenamed('geoHash8','homelocation')

mapped = oct_data.join(homeloc,on='aspkId').dropDuplicates()
mapped.write.mode('append').parquet('s3://staging-near-data-analytics/Adithya/JPN_Tourists_china_korea/{}/refined_analysis/korean_tourists/mar23/refined_mar23_homeloc_mapped_data/'.format(currentdate))


mapped = spark.read.parquet('s3://staging-near-data-analytics/Adithya/JPN_Tourists_china_korea/{}/refined_analysis/korean_tourists/mar23/refined_mar23_homeloc_mapped_data/*'.format(currentdate))
print("devices with HLs in HKG in mar23 month")
print(mapped.select('aspkId').distinct().count()) #


### taking devices which are not having HLs in HKG

hkg_hl = spark.read.parquet('s3://staging-near-data-analytics/Adithya/JPN_Tourists_china_korea/{}/refined_analysis/korean_tourists/mar23/refined_mar23_homeloc_mapped_data/*'.format(currentdate)).select(['aspkId']).dropDuplicates()

# print(hkg_hl.select('aspkId').distinct().count()) #

not_hkg = oct_data.join(hkg_hl,on='aspkId',how='left_anti').dropDuplicates()
not_hkg.write.mode('append').parquet('s3://staging-near-data-analytics/Adithya/JPN_Tourists_china_korea/{}/refined_analysis/korean_tourists/mar23/HLs_not_in_JPN/'.format(currentdate))

not_hkg = spark.read.parquet('s3://staging-near-data-analytics/Adithya/JPN_Tourists_china_korea/{}/refined_analysis/korean_tourists/mar23/HLs_not_in_JPN/*'.format(currentdate))
print("devices not having HLs in HKG for mar23")
print(not_hkg.select('aspkId').distinct().count()) #

#### datediff logic for devices with no HL in HKG
not_hkg = spark.read.parquet('s3://staging-near-data-analytics/Adithya/JPN_Tourists_china_korea/{}/refined_analysis/korean_tourists/mar23/HLs_not_in_JPN/*'.format(currentdate))
data = not_hkg.withColumn('timestamp',from_unixtime("eventTs", "yyyy-MM-dd HH:mm:ss"))
data1 = data.withColumn('date',data.timestamp.substr(1,10))
ft = data1.groupBy('aspkId').agg(datediff(max('date'),min('date')).alias('date_diff'))


sdt = ft.filter((col('date_diff')>=1)&(col('date_diff')<10))
print("devices count between freq 1 to 9 ")
print(sdt.select('aspkId').distinct().count()) #
sdt_ch = sdt.join(data1,on='aspkId').dropDuplicates()
sdt_ch1 = sdt_ch.filter(col('devLanguage')=='ko')
print("devices count between freq 1 to 9 and device lang as korean")
print(sdt_ch1.select('aspkId').distinct().count()) #


sdt_ch1_cnt = sdt_ch1.join(sep,on='aspkId',how='left_anti').dropDuplicates()
sdt_ch1_cnt.write.mode('append').parquet('s3://staging-near-data-analytics/Adithya/JPN_Tourists_china_korea/{}/refined_analysis/korean_tourists_in_JPN/mar23/'.format(currentdate))

sdt_ch1_cnt =spark.read.parquet('s3://staging-near-data-analytics/Adithya/JPN_Tourists_china_korea/{}/refined_analysis/korean_tourists_in_JPN/mar23/*'.format(currentdate))
print("devices count between freq 1 to 9 and device lang as korean and not seen in sep'22")
print(sdt_ch1_cnt.select('aspkId').distinct().count())



df = spark.read.parquet('s3://staging-near-data-analytics/Adithya/JPN_Tourists_china_korea/{}/refined_analysis/korean_tourists_in_JPN/mar23/*'.format(currentdate))
print(df.select('aspkId').distinct().count())
final = df.select('aspkId').distinct()
final.coalesce(1).write.option("header",True).mode('append').csv("s3://staging-near-data-analytics/Adithya/JPN_Tourists_china_korea/{}/refined_analysis/korean_tourists_in_JPN/final_ifas/".format(currentdate))

# Define the URL
url = 'https://int-apigw.adnear.net/allspark-api-v4/internal_apis/v1/custom_people_segment_combo/update?apikey=7badf9cf-1566-4c37-a69b-977512eab1ec'

# Define the request headers
headers = {
'Content-Type': 'application/json'
}

# Define the request payload with the current date
aspkids_file = "s3://staging-near-data-analytics/Adithya/JPN_Tourists_china_korea/{}/refined_analysis/korean_tourists_in_JPN/final_ifas/".format(currentdate)

a = f"aws s3 ls {aspkids_file}"

path = subprocess.check_output(a.split()).decode().split('\n')
for i in path:
    if 'part' in i:
        fin_path = i.split()
        for j in fin_path:
            if 'part' in j:
                final_path = j
print(final_path)

result_path = f"{aspkids_file}{final_path}"
print(result_path)
payload = {
'int_segment_id': 75831,
'refresh': False,
'aspkids_file': result_path
}

# Send the PUT request
response = requests.put(url, headers=headers, json=payload)

# Print the response content
print(response.content.decode('utf-8'))

# Check the response status code
if response.status_code == 200:
    print('Request successful.')
else:
    print('Request failed.')
