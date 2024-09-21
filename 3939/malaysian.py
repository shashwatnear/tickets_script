from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash
import hashlib
import argparse

spark = SparkSession.builder.appName("3932_refined").getOrCreate()

sgp = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3939/refined/SGP/*/*")
sgp = sgp.select(['ifa', 'eventDTLocal'])
sgp = sgp.withColumn('date', col('eventDTLocal').substr(1, 10))
sgp = sgp.groupBy('ifa').agg(countDistinct('date').alias('occurance'))
sgp = sgp.withColumn('occurance', col('occurance').cast('int'))
sgp = sgp.filter(col('occurance') >= 10).orderBy(col('occurance'))
sgp.show()

mys = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3939/refined/MYS/*/*")
mys = mys.select(['ifa', 'eventDTLocal'])
mys = mys.withColumn('date', col('eventDTLocal').substr(1, 10))
mys = mys.groupBy('ifa').agg(countDistinct('date').alias('occurance'))
mys = mys.withColumn('occurance', col('occurance').cast('int'))
mys = mys.filter(col('occurance') >= 10).orderBy(col('occurance'))
mys.show()

result = sgp.join(mys, on = 'ifa', how = 'inner').drop(sgp['occurance']).drop(mys['occurance'])
result = result.dropDuplicates()
result.write.mode("append").parquet("s3://staging-near-data-analytics/shashwat/ps-3939/malaysian/parquet/")

