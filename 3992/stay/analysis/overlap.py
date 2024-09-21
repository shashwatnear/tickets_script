from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *

spark = SparkSession.builder.appName("overlap").getOrCreate()

df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3992/double_radius/poi_footfall/stay/*/*')

# ####### Izmir #######

izmir = ['İzmir CLP', 'İzmir CLP (Savron)', 'İzmir YOVİ TV (ONTV)']
istanbul = ['Metropol CLP', 'Metrobüs CLP Network', 'Seaplay', 'Bağdat Cad. Dijital CLP', 'İcon Dijital (Dijicube)']

for i in izmir:
    for j in izmir:
        d1 = df.filter(col('network') == i)
        d1 = d1.select('ifa')
        d2 = df.filter(col('network') == j)
        d2 = d2.select('ifa')
        d3 = d1.join(d2, on = 'ifa', how = 'inner')
        print(i)
        print(j)
        print(d3.distinct().count())

for i in istanbul:
    for j in istanbul:
        d1 = df.filter(col('network') == i)
        d1 = d1.select('ifa')
        d2 = df.filter(col('network') == j)
        d2 = d2.select('ifa')
        d3 = d1.join(d2, on = 'ifa', how = 'inner')
        print(i)
        print(j)
        print(d3.distinct().count())

