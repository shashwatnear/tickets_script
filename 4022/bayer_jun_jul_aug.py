from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("bayer_jun_jul_aug").getOrCreate()

june = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-4022/Bayer_June.csv', header = True).select(['id', 'FOOTFALL_JUNE'])
june = june.withColumn('id', col('id').cast('int'))
july = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-4022/Bayer_July.csv', header = True)
july = july.withColumn('id', col('id').cast('int'))
august = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-4022/bayer_august/part-00000-9fab61a1-feaf-4992-9298-7ace864ad712-c000.csv', header = True)
august = august.withColumn('id', col('id').cast('int'))

final = july.join(june, on = 'id', how = 'left').select(['id', 'FOOTFALL_JULY', 'FOOTFALL_JUNE'])
# result = august.join(final, on = ['name','category','top_category','telephone','id','neighbourhood','sent','building_door_no','City','Province','address','lon','lat'], how = 'left')
result = august.join(final, on = 'id', how = 'left')
result = result.orderBy('Province')
result = result.select(['name','category','top_category','telephone','id','neighbourhood','sent','building_door_no','City','Province','address','lon','lat', 'FOOTFALL_AUGUST', 'FOOTFALL_JULY', 'FOOTFALL_JUNE'])
result.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-4022/jun_jul_aug/", header = True)

