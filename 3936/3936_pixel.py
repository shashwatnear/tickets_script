from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3936").getOrCreate()

# get attributed footfall
    
def Pixel_Data(start, end, country, tenant_id, datasource_ids):
    dates = get_dates_between(start, end)
    
    for datasource_id in datasource_ids:
        for date in dates:
            try:
                day = "{:02d}".format(date.day)
                month = '{:02d}'.format(date.month)
                year = date.year
                path = "s3://near-pixelmanager/nifi/country={}/id={}/datasource_id={}/year={}/month={}/day={}/*".format(country,tenant_id,datasource_id,year,month,day)
                print(path)
                df = spark.read.json(path)
                df = df.filter(df.datasource_id == "{}".format(datasource_id))
                df = df.select('ncid','istts','datasource_id').distinct()
                df = df.withColumn("istts", col("istts").cast("bigint"))
                df = df.withColumn("istts", df["istts"] / 1000)
                time_offset = -34200
                print(time_offset)
                if time_offset < 0:
                     df = df.withColumn('istts', col("istts") - abs(lit(time_offset))) # Adjusting for negative time difference
                else:
                    df = df.withColumn('istts', col("istts") + time_offset)
                df.show(5)
                print("data for ",date, "is writing..........")
                df.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3936/campaign/pixel/{date}")
                print("Pixel_IDs extraction successfull")
            except Exception as e:
                print(e)
    return "success"



#-----------------------------getting crossmetrix data  (no ifas for pixel so )-------------------------------------------

start = "2024-03-01"
end = "2024-06-18"

pixel_result = Pixel_Data(start, end, "USA", "4d7bb233", ["3402"])