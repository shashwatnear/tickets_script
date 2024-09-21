from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Window
import pyspark.sql.functions as f
from util import *
import argparse
import subprocess

spark = SparkSession.builder.appName("Nine_Automation").getOrCreate()

folder_name = "Nine_Hungry_Jacks"

data = spark.read.csv('s3://staging-near-data-analytics/Nine_files/24189HungryJacksxDynamicAds.csv', header = True).select(['nuid','delivery_timestamp']).dropDuplicates()
data = data.withColumnRenamed('delivery_timestamp','timestamp')
data = data.withColumn('nuid',lower(data.nuid))
print("nine ids count in the shared campaign file")
print(data.select('nuid').distinct().count()) #

pixel = spark.read.parquet('s3://staging-near-data-analytics/Nine_Automation/{}/pixel_data/*/*'.format(folder_name))

data_pixel = pixel.join(data,on='nuid')
print("overlapped nine ids count in the shared campaign file")
print(data_pixel.select('nuid').distinct().count()) #

cross = spark.read.parquet(f's3://staging-near-data-analytics/Nine_Automation/{folder_name}/Cross_matrix_data/*/*').select(['ifa','ncid']).dropDuplicates()
cross = cross.withColumn('ifa',upper(cross.ifa)).withColumn('ncid',lower(cross.ncid))

mapped = cross.join(data_pixel,on='ncid').dropDuplicates()
print("mapped ncids count", mapped.select('ncid').distinct().count())
mapped1 = mapped.select(['ifa','timestamp']).dropDuplicates()
mapped1 = mapped1.withColumn('date',mapped1.timestamp.substr(1,10)).withColumn('time',mapped1.timestamp.substr(12,19))
mapped2 = mapped1.select('*').drop('timestamp')
mapped3 = mapped2.withColumn('timestamp',concat_ws(' ',mapped2.date,mapped2.time))
mapped3 = mapped3.select(['ifa','timestamp']).dropDuplicates()
mapped3 = mapped3.withColumn('timestamp',f.concat(f.col('timestamp'),f.lit('.000')))
mapped3.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/shashwat/ps-3872/mapped3/',header=True)

mapped3 = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3872/mapped3/',header=True)
unique_ids_df = mapped3.dropDuplicates(["ifa"])
unique_ifas = unique_ids_df.distinct().count()

if unique_ifas < 2800000:
    result_df = mapped3.withColumn("row_number", row_number().over(Window.partitionBy("ifa").orderBy("timestamp"))) \
        .filter(col("row_number") <= 10) \
        .drop("row_number")
    result_df = result_df.limit(2800000)
    result_df.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/Nine_Automation/{}/final_exposed_file/'.format(folder_name))
else:
    unique_ids_df.coalesce(1).write.mode('append').csv('s3://staging-near-data-analytics/Nine_Automation/{}/final_exposed_file/'.format(folder_name))