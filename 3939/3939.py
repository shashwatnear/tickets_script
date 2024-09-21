from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *

spark = SparkSession.builder.appName("3939").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

start = '2024-05-01'
end = '2024-05-31'

dates = get_dates_between(start, end)

# Footfall report
for date in dates:
    try:
        day = "{:02d}".format(date.day)

        data = spark.read.parquet(f"s3://near-data-warehouse/refined/dataPartner=*/year=2024/month=05/day={day}/hour=*/country=SGP/*")
        data = data.select(['ifa', data.geoHash9.substr(1,8).alias("geohash"), 'coreSeg', 'eventDTLocal'])
        data.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3939/refined/{date}")
    except:
        print(date, " Path does not exists")