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
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pyspark.sql.types as T

spark = SparkSession.builder.appName("Tourist cards").getOrCreate()
sc = spark.sparkContext

# # for hkg we took 15 days of data
# import datetime
# currentdate = datetime.datetime.now().strftime("%Y-%m-%d")
# print(currentdate)

# from datetime import datetime
# datelist = pd.date_range(end=datetime.today(), periods=100).tolist()

# dates = []
# for i in datelist:
#     res = str(i.date())
#     dates.append(res)


# countries = ['HKG','AUS','IDN','JPN','MYS','NZL','PHL','SGP','THA','TWN']
# for country in countries:
#     if country == "HKG":
#         for date in dates[-30:]:
#             try:
#                 path = "s3://near-rule-engine-data/rule-input/{}/{}".format(country,date)
#                 print(path) 
#                 data = spark.read.parquet(path)
#                 data_footfall = data.select(["aspkId",'v_date']).dropDuplicates()
#                 data_footfall.write.mode("append").parquet("s3://staging-near-data-analytics/Kalyan/ps_3234/{}/rule_engg/{}/{}/".format(currentdate,country,date))
#             except Exception as e:
#                 print(e)
#     else:
#         for date in dates[-90:]:
#             try:
#                 path = "s3://near-rule-engine-data/rule-input/{}/{}".format(country,date)
#                 print(path)
#                 data = spark.read.parquet(path)
#                 data_footfall = data.select(["aspkId",'v_date']).dropDuplicates()
#                 data_footfall.write.mode("append").parquet("s3://staging-near-data-analytics/Kalyan/ps_3234/{}/rule_engg/{}/{}/".format(currentdate,country,date))
#             except Exception as e:
#                 print(e)


# schema1 = StructType([StructField("aspkId", StringType(), False),
#                       StructField("HomeLocation", StringType(), True),
#                       StructField("new_days", StringType(),True) ])

# df = spark.createDataFrame(data = sc.emptyRDD(), schema=schema1)

# hkg = spark.read.parquet("s3://staging-near-data-analytics/Kalyan/ps_3234/{}/rule_engg/HKG/*/*".format(currentdate)).dropDuplicates()
# hkg = hkg.withColumnRenamed('v_date','hkg_date')
# hkg_cnts = hkg.groupBy('aspkId').agg(countDistinct('hkg_date').alias('hkg_count'))
# hkg_min = hkg.groupBy('aspkId').agg(min('hkg_date').alias('hkg_min_date'))                                                    
# hkg_cnts_min = hkg_cnts.join(hkg_min, on='aspkId', how='inner').dropDuplicates()
# hkg_max = hkg.groupBy('aspkId').agg(max('hkg_date').alias('max_date'))
# hkg_cnts_min_max = hkg_cnts_min.join(hkg_max, on='aspkId', how='inner').dropDuplicates()
# hkg_fin = hkg_cnts_min_max.withColumnRenamed('max_date','hkg_max_date').withColumnRenamed('hkg_count', 'hkg_days')
# hkg_fin = hkg_fin.withColumn('diff_hkg', datediff('hkg_max_date', 'hkg_min_date')) 

# countries = ['IDN','JPN','MYS','NZL','PHL','SGP','THA','TWN']
# for country in countries:
#     new = spark.read.parquet("s3://staging-near-data-analytics/Kalyan/ps_3234/{}/rule_engg/{}/*/*".format(currentdate,country)).dropDuplicates()
#     new = new.withColumnRenamed('v_date','{}_date'.format(country))
#     new_cnts = new.groupBy('aspkId').agg(countDistinct('{}_date'.format(country)).alias('{}_days'.format(country)))
#     new_min = new.groupBy('aspkId').agg(min('{}_date'.format(country)).alias('{}_min_date'.format(country)))
#     new_cnts_min = new_cnts.join(new_min, on='aspkId', how='inner').dropDuplicates()
#     new_max = new.groupBy('aspkId').agg(max('{}_date'.format(country)).alias('{}_max_date'.format(country)))
#     new_fin = new_cnts_min.join(new_max, on='aspkId', how='inner').dropDuplicates()
#     new_fin = new_fin.withColumn('diff_{}_date'.format(country), datediff('{}_max_date'.format(country), '{}_min_date'.format(country)))
#     data = hkg_fin.join(new_fin, on='aspkId', how='inner').dropDuplicates()
#     data1 = data.filter( (col('hkg_days')>1) & (col('{}_days'.format(country))>1))
#     data1 = data1.withColumn('HomeLocation', when(col('hkg_days') >= col('{}_days'.format(country)), "HKG").otherwise("{}".format(country)))
#     data1 = data1.withColumn('new_days', (col('{}_days'.format(country)) - col('hkg_days'))).select('aspkId','HomeLocation','new_days').dropDuplicates()
#     df = df.union(data1)
#     df = df.dropDuplicates()

# df.write.mode('append').parquet("s3://staging-near-data-analytics/Kalyan/ps_3234/{}/New_tourist_set_90days".format(currentdate))

currentdate = "2024-07-04"

# da = spark.read.parquet("s3://staging-near-data-analytics/Kalyan/ps_3234/{}/New_tourist_set_90days/*".format(currentdate))
# da1 = da.filter(~(col('HomeLocation')=="HKG"))

# # AUS
# aus_tc = da1.filter(col('HomeLocation').isin(['IDN','JPN','MYS','NZL','PHL','SGP','THA','TWN'])).filter(col('new_days')>=1).select('aspkId','HomeLocation').distinct()
# aus_tc.count()
# aus_tc.coalesce(1).write.partitionBy('HomeLocation').mode('append').csv("s3://staging-near-data-analytics/Kalyan/ps_3234/{}/tc/".format(currentdate))


a = ['aws', 's3api', 'list-objects', '--bucket', 'staging-near-data-analytics' ,'--prefix', 'Kalyan/ps_3234/{}/tc/'.format(currentdate), '--output', 'text', '--query', 'Contents[].{Key: Key}']
res = subprocess.check_output(a)

new = res.decode()
new_res = new.split('\n')

fin=[]
countries = ['IDN','JPN','MYS','NZL','PHL','SGP','THA','TWN']
for country in countries:
    for path in new_res:
        if country in path:
            fin.append(path)


seg_ids = [25396,25394,25392,25391,25397,25408,25393,72094]
for seg_id,path in zip(seg_ids,fin):
    data = {"int_segment_id": "{}".format(seg_id),
            "refresh": "false",
            "aspkids_file": "s3://staging-near-data-analytics/{}".format(path)}
    headers = {"Content-type": "application/json"}
    resp = requests.put("https://int-apigw.adnear.net/allspark-api-v4/internal_apis/v1/custom_people_segment_combo/update?apikey=6b4912ed-9063-4d20-9886-755ee95f95b7", json = data, headers = headers)
    print(seg_id,"s3://staging-near-data-analytics/{}".format(path))
    print(f"Created PC CARD :{seg_id}")
    # print("URL:", resp.json()['payload']['status_url'])
    print("URL:", resp.json())
    print('----------------------------------')
    time.sleep(5)