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


spark = SparkSession.builder.appName("Tourist cards for Australia").getOrCreate()

currentdate = '2024-07-01'
df = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3948/aus/Tourist_card/{}/Tourist_90_data/*/*".format(currentdate))
df1 = df.filter(col('HomeLocation') != "Australia")

df_1 = df1.filter(col('new_days') >= 1).withColumnRenamed('HomeLocation','country').filter((col('country').isin(['ARE', 'CAN', 'DEU', 'ESP', 'FRA', 'GBR', 'HKG', 'IDN', 'IND', 'IRL', 'ITA', 'JPN', 'MEX', 'MYS', 'NLD', 'NZL', 'PHL', 'SAU', 'SGP', 'THA', 'TUR', 'TWN', 'USA', 'VNM'])))

countries = ['ARE', 'CAN', 'DEU', 'ESP', 'FRA', 'GBR', 'HKG', 'IDN', 'IND', 'IRL', 'ITA', 'JPN', 'MEX', 'MYS', 'NLD', 'NZL', 'PHL', 'SAU', 'SGP', 'THA', 'TUR', 'TWN', 'USA', 'VNM']

for country in countries:
    temp = df_1.filter(col('country') == country).select('aspkId')
    temp.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-3948/temp/aus/aspkid_to_create_cards/{country}/", header = True)




# curl -XPOST --header "Content-type: application/json" --data '{"settings":{"visit_period":90},"insights":true, "name":"{} Inbound", "country":"AUS", "file": ".csv"}' https://int-apigw.adnear.net/allspark-api-v4/internal_apis/v1/custom_people_segment_combo?apikey=e4fec7a2-40fc-47b5-83a3-31156fa5be02
# curl -XPOST --header "Content-type: application/json" --data '{"settings":{"visit_period":90},"insights":true, "name":"{} Inbound", "country":"NZL", "file": ".csv"}' https://int-apigw.adnear.net/allspark-api-v4/internal_apis/v1/custom_people_segment_combo?apikey=967a79c7-9768-4b38-88c8-035eedef67d6

# file = ? .csv
# name = ?
