##postalcode scale newskeys###
from pyspark.sql.functions import *
from datetime import date,timedelta,datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import datetime
from datetime import datetime, timedelta
import requests,json
import pandas as pd
import proximityhash
import Geohash
from dateutil.parser import parse
from pyspark.sql.functions import explode, col
from pyspark.sql.window import Window
from datetime import date, timedelta
import pandas as pd
import boto3
import os

spark = SparkSession.builder.appName("newscorp_deliverable").getOrCreate()

segId = [86580, 87641, 85747, 85418, 86768, 86714, 86670, 86765, 86638, 85200, 87512, 85969, 86363, 87418, 86922]

# # part1

# for i in segId:
#     aa = spark.read.parquet("s3://near-rule-engine-data/rule-output/segment={}/date=2024-05-*/".format(i))
#     aa1 = aa.select("aspkId").distinct()
#     aa1 = aa1.withColumn("segId", lit(i))
#     aa1.write.parquet("s3://staging-near-data-analytics/Chandrashekhar/3938/postcode_final/aspk_june/{}/".format(i))
#     print(i)

# # part2

# #segId<>ifa<>aspkid mapping using id-manager dump#
# aa = spark.read.parquet("s3://staging-near-data-analytics/Chandrashekhar/3938/postcode_final/aspk_june/*/*/")

# #id_man = spark.read.parquet("s3://staging-near-data-analytics/Chandrashekhar/id_man_dump_2/")
# id_man = spark.read.parquet("s3://near-data-warehouse/id-manager-offline/current/")
# id_man = id_man.select("aspk_id", "ifa").distinct()

# final = aa.join(id_man, aa.aspkId == id_man.aspk_id, "inner").drop(id_man.aspk_id)

# final.write.parquet("s3://staging-near-data-analytics/Chandrashekhar/3938/postcode_final/ifa_segId_aspkId_june/")

# # part3

# aa = spark.read.parquet("s3://staging-near-data-analytics/Chandrashekhar/3938/postcode_final/ifa_segId_aspkId_june/")
# aa  = aa.withColumn("ifa", lower(col("ifa")))

# nk = spark.read.csv("s3://near-carbonplatform-data/internal/ifa-tpid-dataset/tenantId=4d5cc3a0/dsId=1178/current/", header = True)

# nk = nk.withColumn("tpids",explode(split(col("tpids"),'\\|')))
# nk  = nk.withColumn("ifa", lower(col("ifa")))

# final = aa.join(nk, nk.ifa == aa.ifa, "inner").drop(nk.ifa)

# final1 = final.select('segId', 'ifa', 'tpids').distinct()

# window_spec = Window.partitionBy("segId", "ifa").orderBy("tpids")
# final1 = final1.withColumn("row_num", row_number().over(window_spec))

# #adding cutoffs 2#
# window_spec_2 = Window.partitionBy("segId", "ifa")
# final2 = final1.withColumn("max_row_num", max('row_num').over(window_spec_2))

# final3 = final2.filter(final2.max_row_num <= 8)

# # part4

# #writing the data#
# final4 = final3.select("segId", "tpids").distinct()

# for i in segId:
#     final5 = final4.filter(final4.segId == i)
#     final5.select("tpids").distinct().repartition(1).write.parquet("s3://staging-near-data-analytics/Chandrashekhar/3938/postcode_final/segments/{}/".format(i))
#     print(i)

# part5
    
# # File shared by chandrashekar
# segmentId_cardId = spark.read.csv('s3://staging-near-data-analytics/shashwat/chandra/AudienceCards_Near_20240517.csv', header = True)

# for segment in segId:
#     segmentId_tpid = spark.read.parquet(f's3://staging-near-data-analytics/Chandrashekhar/3938/postcode_final/segments/{segment}/*')
#     segmentId_tpid = segmentId_tpid.withColumnRenamed('tpids', 'fpid')
#     segmentId_tpid = segmentId_tpid.withColumn('segment_id', lit(segment))
#     final = segmentId_tpid.join(segmentId_cardId, on = 'segment_id', how = 'inner')
#     final = final.select(['segment_id', 'card_id', 'fpid'])
#     final.repartition(1).write.parquet(f"s3://staging-near-data-analytics/Chandrashekhar/3938/postcode_final/cardId_segmentId_fpid/{segment}/")
#     print("Finished segment", segment)

# part6 

final = spark.read.parquet('s3://staging-near-data-analytics/Chandrashekhar/3938/postcode_final/cardId_segmentId_fpid/*/*')
card_ids = final.select('card_id').distinct().collect()

# Create an S3 client
s3_client = boto3.client('s3')
bucket_name = 'staging-near-data-analytics'


# ############################### Writing as parquet file ##################
  
for row in card_ids:
  card_id = row['card_id']
    
  # Filter the dataframe for the current card_id
  df_filtered = final.filter(final.card_id == card_id)
  df_filtered = df_filtered.select('fpid').distinct()
  
  # Convert the filtered Spark DataFrame to a Pandas DataFrame
  df_filtered_pd = df_filtered.toPandas()
  
  # Write DataFrame to Parquet file with Snappy compression
  output_file_path = f's3://staging-near-data-analytics/Chandrashekhar/3938/postcode_final/20240613/near_nc_segments_whitelist_{card_id}_001_20240613_00001.snappy.parquet'
  output_dir = os.path.dirname(output_file_path)
  os.makedirs(output_dir, exist_ok=True)
  df_filtered_pd.to_parquet(output_file_path, compression='snappy')

  print(f"DataFrame successfully written to {output_file_path}")