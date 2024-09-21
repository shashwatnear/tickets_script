from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *

spark = SparkSession.builder.appName("jpn_21_countries").getOrCreate()

countries = ['JPN','MYS', 'NLD', 'NZL', 'PHL', 'SAU', 'SGP', 'THA', 'TUR','VNM','IND','IDN','ARE','IRL','DEU','FRA','GBR','ESP','HKG','ITA','AUS','TWN','CAN','MEX']
for country in countries:
    try:
        print("Current country", country)
        df = spark.read.parquet(f"s3://staging-near-data-analytics/Adithya/3732/Tourist_card/2024-07-10/Tourist_90_data/{country}/*")
        df = df.filter(col('HomeLocation') == f'{country}')
        # df = df.filter(col('new_days') >= 1)
        df.agg(countDistinct('aspkId')).show()
    except:
        print("Path does not exists: ", country)

spark.stop()
