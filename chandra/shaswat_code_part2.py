##postalcode scale newskeys###
from pyspark.sql.functions import *
from datetime import date,timedelta,datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import datetime
from datetime import datetime, timedelta
import requests,json
import pandas as pd
import proximityhash
import Geohash
from dateutil.parser import parse
from pyspark.sql.functions import explode, col
from pyspark.sql.window import Window
from datetime import date, timedelta

spark = SparkSession.builder.appName("test").config("spark.sql.autoBroadcastJoinThreshold", -1).getOrCreate()
sc = spark.sparkContext

aa = spark.read.parquet("s3://staging-near-data-analytics/Chandrashekhar/postcode_final/aspk_april/*/*/")

id_man = spark.read.parquet("s3://near-data-warehouse/id-manager-offline/current/")
id_man = id_man.select("aspk_id", "ifa").distinct()

final = aa.join(id_man, aa.aspkId == id_man.aspk_id, "inner").drop(id_man.aspk_id)

final.write.parquet("s3://staging-near-data-analytics/Chandrashekhar/postcode_final/ifa_segId_aspkId_april/")