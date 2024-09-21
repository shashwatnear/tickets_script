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

spark = SparkSession.builder.appName("updating_jpn_usa_tourist_card").getOrCreate()

currentdate = "2024-05-28"

df = spark.read.parquet("s3://staging-near-data-analytics/Adithya/3732/USA_Tourist_card/{}/Tourist_90_data/*/*".format(currentdate))
df1 = df.filter(col('HomeLocation')!="Japan")

df_1 = df1.filter(col('new_days')>=1).withColumnRenamed('HomeLocation','country').filter((col('country').isin(['USA'])))

card_info = spark.read.csv("s3://near-data-analytics/adithya/USA_tourist_card_xl_jpn.csv", header=True).select('seg_id','country').dropDuplicates()

data1 = df_1.join(card_info, on='country', how='inner').dropDuplicates()

data1.select('aspkId','seg_id').coalesce(1).write.partitionBy('seg_id').mode('append').csv("s3://staging-near-data-analytics/Adithya/3732/USA_Tourist_card/{}/Tourist_card_seg/".format(currentdate))


a = ['aws', 's3api', 'list-objects', '--bucket', 'staging-near-data-analytics' ,'--prefix', 'Adithya/3732/USA_Tourist_card/{}/Tourist_card_seg/'.format(currentdate), '--output', 'text', '--query', 'Contents[].{Key: Key}']
res = subprocess.check_output(a)

new = res.decode()
new_res = new.split('\n')

fin=[]
seg_ids = [69986]
for seg_id in seg_ids:
    for path in new_res:
        if str(seg_id) in path:
            fin.append(path)

for seg_id,path in zip(seg_ids,fin):
    data = {"int_segment_id": "{}".format(seg_id),
            "refresh": "false",
            "aspkids_file": "s3://staging-near-data-analytics/{}".format(path)}
    headers = {"Content-type": "application/json"}
    resp = requests.put("https://int-apigw.adnear.net/allspark-api-v4/internal_apis/v1/custom_people_segment_combo/update?apikey=7badf9cf-1566-4c37-a69b-977512eab1ec", json = data, headers = headers)
    print(seg_id,"s3://staging-near-data-analytics/{}".format(path))
    print(f"Created PC CARD :{seg_id}")
    print("URL:", resp.json()['payload']['status_url'])
    print()
    print('----------------------------------')
    time.sleep(3)