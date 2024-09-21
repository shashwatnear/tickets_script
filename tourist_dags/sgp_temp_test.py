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

spark = SparkSession.builder.appName("Singapore - Tourist cards refresh").getOrCreate()
sc = spark.sparkContext

currentdate = '2024-08-02'

# start = '2024-06-20'
# end = '2024-07-20'

# dates = get_dates_between(start, end)

# countries = ['SGP']

# for country in countries:
#     for date in dates:
#         try:
#             path = "s3://near-rule-engine-data/rule-input/{}/{}".format(country,date)
#             data = spark.read.parquet(path)
#             data = data.where(col("hdp_flag")==0)
#             data_footfall = data.select(["aspkId",'v_date']).dropDuplicates()
#             data_footfall.write.mode("append").parquet("s3://staging-near-data-analytics/tourist_dags_data/sgp_temp/rule_engg/{}/{}/".format(country,date))
#         except Exception as e:
#             print(e)

# ###############################

# start = '2024-04-20'
# end = '2024-07-20'

# dates = get_dates_between(start, end)

# countries = ['ARE', 'AUS', 'CAN', 'DEU', 'ESP', 'FRA', 'GBR', 'HKG', 'IDN', 'IND', 'IRL', 'ITA', 'JPN', 'MEX', 'MYS', 'NLD', 'NZL', 'PHL', 'SAU', 'THA', 'TUR', 'TWN', 'USA', 'VNM']

# for country in countries:
#     for date in dates:
#         try:
#             path = "s3://near-rule-engine-data/rule-input/{}/{}".format(country,date)
#             data = spark.read.parquet(path)
#             data = data.where(col("hdp_flag")==0)
#             data_footfall = data.select(["aspkId",'v_date']).dropDuplicates()
#             data_footfall.write.mode("append").parquet("s3://staging-near-data-analytics/tourist_dags_data/sgp_temp/rule_engg/{}/{}/".format(country,date))
#         except Exception as e:
#             print(e)


###############################

# sgp = spark.read.parquet("s3://staging-near-data-analytics/tourist_dags_data/sgp_temp/rule_engg/SGP/*/*").dropDuplicates()
# sgp = sgp.withColumnRenamed('v_date','sgp_date')

# sgp_cnts = sgp.groupBy('aspkId').agg(countDistinct('sgp_date').alias('sgp_days'))

# countries = ['ARE', 'AUS', 'CAN', 'DEU', 'ESP']

# for country in countries:
#     new = spark.read.parquet("s3://staging-near-data-analytics/tourist_dags_data/sgp_temp/rule_engg/{}/*/*".format(country)).dropDuplicates()
#     new = new.withColumnRenamed('v_date','{}_date'.format(country))
 
#     new_cnts = new.groupBy('aspkId').agg(countDistinct('{}_date'.format(country)).alias('{}_days'.format(country)))

#     data = sgp_cnts.join(new_cnts, on='aspkId', how='inner').dropDuplicates()

#     data1 = data.filter( (col('sgp_days')>1) & (col('{}_days'.format(country))>1))
#     data1 = data1.withColumn('HomeLocation', when(col('sgp_days') >= col('{}_days'.format(country)), "Singapore").otherwise("{}".format(country)))
#     data1 = data1.withColumn('new_days', (col('{}_days'.format(country)) - col('sgp_days'))).select('aspkId','HomeLocation','new_days').dropDuplicates()
#     data1.write.mode('append').parquet("s3://staging-near-data-analytics/tourist_dags_data/sgp_temp/Tourist_card/{}/Tourist_90_data/{}".format(currentdate, country))

###############################
    
# df = spark.read.parquet("s3://staging-near-data-analytics/tourist_dags_data/sgp_temp/Tourist_card/{}/Tourist_90_data/*/*".format(currentdate))
# df1 = df.filter(col('HomeLocation') != "Singapore")

# df_1 = df1.filter(col('new_days')>=1).withColumnRenamed('HomeLocation','country').filter((col('country').isin(['ARE', 'AUS', 'CAN', 'DEU', 'ESP'])))

# card_info = spark.read.csv("s3://near-data-analytics/segment_ids_tourist_cards/sgp_segment_ids.csv", header = True).select('segment_id', 'country').dropDuplicates()

# data1 = df_1.join(card_info, on='country', how='inner').dropDuplicates()

# data1.select('aspkId','segment_id').coalesce(1).write.partitionBy('segment_id').mode('append').csv("s3://staging-near-data-analytics/tourist_dags_data/sgp_temp/Tourist_card/{}/Tourist_card_seg/".format(currentdate))

###############################


a = ['aws', 's3api', 'list-objects', '--bucket', 'staging-near-data-analytics' ,'--prefix', 'tourist_dags_data/sgp_temp/Tourist_card/{}/Tourist_card_seg/'.format(currentdate), '--output', 'text', '--query', 'Contents[].{Key: Key}']
res = subprocess.check_output(a)

new = res.decode()
new_res = new.split('\n')

fin=[]
# seg_ids = [90813,90810,90811,90809,90808]
seg_ids = [90809]
for seg_id in seg_ids:
    for path in new_res:
        if str(seg_id) in path:
            fin.append(path)

for seg_id,path in zip(seg_ids,fin):
    data = {"int_segment_id": "{}".format(seg_id),
            "refresh": "false",
            "aspkids_file": "s3://staging-near-data-analytics/{}".format(path)}
    headers = {"Content-type": "application/json"}
    resp = requests.put("https://int-apigw.adnear.net/allspark-api-v4/internal_apis/v1/custom_people_segment_combo/update?apikey=3db611d5-a3ee-4a80-bfc5-f7b1e00b6e7f", json = data, headers = headers)
    print(seg_id,"s3://staging-near-data-analytics/{}".format(path))
    print(f"Created PC CARD :{seg_id}")
    print("URL:", resp.json()['payload']['status_url'])
    print()
    print('----------------------------------')
    time.sleep(3)