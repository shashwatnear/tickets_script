from pyspark.sql import SparkSession
import geohash
from polygon_geohasher.polygon_geohasher import polygon_to_geohashes, geohashes_to_polygon
import json
from shapely.geometry import shape, mapping
import numpy as np
import pyspark.sql.functions as F
import pyspark.sql.types as T
import s3fs
import boto3
from datetime import datetime, timedelta
from util import *

spark = SparkSession.builder.appName("pixel_cross").getOrCreate()

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
                df.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3929/campaign/pixel/{date}")
                print("Pixel_IDs extraction successfull")
            except Exception as e:
                print(e)
    return "success"



#-----------------------------getting crossmetrix data  (no ifas for pixel so )-------------------------------------------

def Cross_Matrix_Data(start, end, country, tenant_id, datasource_ids):
    dates = get_dates_between(start, end)
    for datasource_id in datasource_ids:
        for date in dates:
            try:
                day = "{:02d}".format(date.day)
                month = '{:02d}'.format(date.month)
                year = date.year
                id1 = spark.read.parquet("s3://near-datamart/universal_cross_matrix/compass/linkages/country={}/tenant_id={}/datasource_id={}/year={}/month={}/day={}/".format(country,tenant_id,datasource_id,year,month,day))
                print(id1)
                cross1 = id1.filter(id1.from_type=='ifa')
                cross2 = id1.filter(id1.from_type=='ncid')
                cross3 = cross1.withColumnRenamed('from','ifa').withColumnRenamed('to','ncid')
                cross4 = cross2.withColumnRenamed('from','ncid').withColumnRenamed('to','ifa')
                final_cross = cross3.unionByName(cross4).dropDuplicates()
                final_cross.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3929/campaign/cross/{date}")
                print("Crossmatrix_IDs extraction successfull")
            except Exception as e:
                print(e)
    return "success"

start = "2024-01-15"
end = "2024-05-15"

pixel_result = Pixel_Data(start, end, "USA", "4d7bb233", ["3244"])
cross_result = Cross_Matrix_Data(start, end, "USA", "4d7bb233", ["3244"])