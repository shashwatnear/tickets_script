from datetime import date,timedelta, datetime
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("392_sgp").getOrCreate()

# start = "2023-11-01"
# end = "2024-04-30"
# country = "SGP"

# dates = get_dates_between(start, end)

# # Footfall report
# for date in dates:
#     day = "{:02d}".format(date.day)
#     month = '{:02d}'.format(date.month)
#     year = date.year
#     dest_path = f"s3://staging-near-data-analytics/shashwat/ps-3922/devLanguageMS/sgp/{date}/"
#     print(dest_path)

#     data = spark.read.parquet("s3://near-data-warehouse/refined/dataPartner=*/year={}/month={}/day={}/hour=*/country={}/*".format(year,month,day,country))
#     data = data.filter(col('devLanguage') == 'ms')
#     data = data.select(['aspkId'])
#     data.write.mode("append").parquet(dest_path)

# ##################################

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3922/devLanguageMS/sgp/*/*')
df = df.select('aspkId').distinct()
df.coalesce(1).write.mode('overwrite').csv(f"s3://staging-near-data-analytics/shashwat/ps-3922/reports/sgp/")