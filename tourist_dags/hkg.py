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
# from util import *
import boto3
import pandas as pd
import datetime
from datetime import datetime,timedelta

spark = SparkSession.builder.appName("HongKong - Tourist cards refresh").getOrCreate()
sc = spark.sparkContext

# for hkg we take 30 days of data
currentdate = datetime.now().strftime("%Y-%m-%d")
print(currentdate)

end_date = datetime.today() - timedelta(days=10)
start_date = end_date - timedelta(days=30)
datelist = pd.date_range(start=start_date, end=end_date).tolist()

dates = []
for i in datelist:
    res = str(i.date())
    dates.append(res)

countries = ['HKG']

for country in countries:
    for date in dates:
        try:
            path = "s3://near-rule-engine-data/rule-input/{}/{}".format(country,date)
            data = spark.read.parquet(path)
            data_footfall = data.select(["aspkId",'v_date']).dropDuplicates()
            data_footfall.write.mode("append").parquet("s3://staging-near-data-analytics/tourist_dags_data/hkg/rule_engg/{}/{}/".format(country,date))
        except Exception as e:
            print(e)

# ###############################

end_date = datetime.today() - timedelta(days=10)
start_date = end_date - timedelta(days=90)
datelist = pd.date_range(start=start_date, end=end_date).tolist()

dates = []
for i in datelist:
    res = str(i.date())
    dates.append(res)

countries = ['ARE', 'AUS', 'CAN', 'DEU', 'ESP', 'FRA', 'GBR', 'IDN', 'IND', 'IRL', 'ITA', 'JPN', 'MEX', 'MYS', 'NLD', 'NZL', 'PHL', 'SAU', 'SGP', 'THA', 'TUR', 'TWN', 'USA', 'VNM']

for country in countries:
    for date in dates:
        try:
            path = "s3://near-rule-engine-data/rule-input/{}/{}".format(country,date)
            data = spark.read.parquet(path)
            data_footfall = data.select(["aspkId",'v_date']).dropDuplicates()
            data_footfall.write.mode("append").parquet("s3://staging-near-data-analytics/tourist_dags_data/hkg/rule_engg/{}/{}/".format(country,date))
        except Exception as e:
            print(e)


###############################

hkg = spark.read.parquet("s3://staging-near-data-analytics/tourist_dags_data/hkg/rule_engg/HKG/*/*").dropDuplicates()
hkg = hkg.withColumnRenamed('v_date','hkg_date')

hkg_cnts = hkg.groupBy('aspkId').agg(countDistinct('hkg_date').alias('hkg_days'))

countries = ['ARE', 'AUS', 'CAN', 'DEU', 'ESP', 'FRA', 'GBR', 'IDN', 'IND', 'IRL', 'ITA', 'JPN', 'MEX', 'MYS', 'NLD', 'NZL', 'PHL', 'SAU', 'SGP', 'THA', 'TUR', 'TWN', 'USA', 'VNM']

for country in countries:
    new = spark.read.parquet("s3://staging-near-data-analytics/tourist_dags_data/hkg/rule_engg/{}/*/*".format(country)).dropDuplicates()
    new = new.withColumnRenamed('v_date','{}_date'.format(country))
 
    new_cnts = new.groupBy('aspkId').agg(countDistinct('{}_date'.format(country)).alias('{}_days'.format(country)))

    data = hkg_cnts.join(new_cnts, on='aspkId', how='inner').dropDuplicates()

    data1 = data.filter( (col('hkg_days')>1) & (col('{}_days'.format(country))>1))
    data1 = data1.withColumn('HomeLocation', when(col('hkg_days') >= col('{}_days'.format(country)), "HongKong").otherwise("{}".format(country)))
    data1 = data1.withColumn('new_days', (col('{}_days'.format(country)) - col('hkg_days'))).select('aspkId','HomeLocation','new_days').dropDuplicates()
    data1.write.mode('append').parquet("s3://staging-near-data-analytics/tourist_dags_data/hkg/Tourist_card/{}/Tourist_90_data/{}".format(currentdate, country))

###############################
    
df = spark.read.parquet("s3://staging-near-data-analytics/tourist_dags_data/hkg/Tourist_card/{}/Tourist_90_data/*/*".format(currentdate))
df1 = df.filter(col('HomeLocation') != "HongKong")

df_1 = df1.filter(col('new_days')>=1).withColumnRenamed('HomeLocation','country').filter((col('country').isin(['ARE', 'AUS', 'CAN', 'DEU', 'ESP', 'FRA', 'GBR', 'IDN', 'IND', 'IRL', 'ITA', 'JPN', 'MEX', 'MYS', 'NLD', 'NZL', 'PHL', 'SAU', 'SGP', 'THA', 'TUR', 'TWN', 'USA', 'VNM'])))

card_info = spark.read.csv("s3://near-data-analytics/segment_ids_tourist_cards/hkg_segment_ids.csv", header = True).select('segment_id', 'country').dropDuplicates()

data1 = df_1.join(card_info, on='country', how='inner').dropDuplicates()

data1.select('aspkId','segment_id').coalesce(1).write.partitionBy('segment_id').mode('append').csv("s3://staging-near-data-analytics/tourist_dags_data/hkg/Tourist_card/{}/Tourist_card_seg/".format(currentdate))

###############################


a = ['aws', 's3api', 'list-objects', '--bucket', 'staging-near-data-analytics' ,'--prefix', 'tourist_dags_data/hkg/Tourist_card/{}/Tourist_card_seg/'.format(currentdate), '--output', 'text', '--query', 'Contents[].{Key: Key}']
res = subprocess.check_output(a)

new = res.decode()
new_res = new.split('\n')

fin=[]
seg_ids = [90792,90791,90790,90789,90788,90786,90787,90784,90785,90783,90782,90780,90781,90778,90779,90776,90777,90774,90775,90772,90773,90771,90770,90769]
for seg_id in seg_ids:
    for path in new_res:
        if str(seg_id) in path:
            fin.append(path)

for seg_id,path in zip(seg_ids,fin):
    data = {"int_segment_id": "{}".format(seg_id),
            "refresh": "false",
            "aspkids_file": "s3://staging-near-data-analytics/{}".format(path)}
    headers = {"Content-type": "application/json"}
    resp = requests.put("https://int-apigw.adnear.net/allspark-api-v4/internal_apis/v1/custom_people_segment_combo/update?apikey=6b4912ed-9063-4d20-9886-755ee95f95b7", json = data, headers = headers)
    print(seg_id,"s3://staging-near-data-analytics/{}".format(path))
    print(f"Created PC CARD :{seg_id}")
    print("URL:", resp.json()['payload']['status_url'])
    print()
    print('----------------------------------')
    time.sleep(3)