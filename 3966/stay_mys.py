from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *

spark = SparkSession.builder.appName("3966").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

start = '2024-06-01'
end = '2024-06-30'

dates = get_dates_between(start, end)

# Footfall report
for date in dates:
    try:
        day = "{:02d}".format(date.day)
        data = spark.read.parquet(f"s3://near-datamart/staypoints/version=*/dataPartner=*/year=2024/month=06/day={day}/hour=*/country=MYS/*")
        data = data.select(['ifa', 'eventDTLocal'])
        data.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3966/mys_gh8_staypoints_data1/{date}/")
    except:
        print(date, " Path does not exists")