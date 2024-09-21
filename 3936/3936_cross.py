from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash

spark = SparkSession.builder.appName("3936").getOrCreate()

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
                final_cross.write.mode("append").parquet(f"s3://staging-near-data-analytics/shashwat/ps-3936/campaign/cross/{date}")
                print("Crossmatrix_IDs extraction successfull")
            except Exception as e:
                print(e)
    return "success"

start = "2024-03-01"
end = "2024-06-18"

cross_result = Cross_Matrix_Data(start, end, "USA", "4d7bb233", ["3402"])