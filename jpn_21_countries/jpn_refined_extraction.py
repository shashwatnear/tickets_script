from pyspark.sql.functions import *
from datetime import date,timedelta,datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import datetime
from datetime import datetime, timedelta
from pyspark.sql.window import Window
import requests,json
import pandas as pd
import proximityhash
import Geohash
import pyspark.sql.functions as f
import pytz
from util import *
from calendar import monthrange
from pyspark.sql.functions import col
import subprocess


spark = SparkSession.builder.appName("test").getOrCreate()
sc = spark.sparkContext

import datetime
currentdate = datetime.datetime.now().strftime("%Y-%m-%d")
print(currentdate)

from datetime import datetime
datelist = pd.date_range(end=datetime.today(), periods=65).tolist()

dates = []
for i in datelist:
    res = str(i.date())
    dates.append(res)

for date in dates[-65:]:
	try:
		date_obj = datetime.strptime(date, "%Y-%m-%d")
		day = "{:02d}".format(date_obj.day)
		month = '{:02d}'.format(date_obj.month)
		year = date_obj.year 
		path = "s3://near-data-warehouse/refined/dataPartner=*/year={}/month={}/day={}/hour=*/country=JPN/*".format(year,month,day)
		print(path)
		data = spark.read.parquet(path)
		data_footfall = data.select(['aspkId','devLanguage','eventTs','devCarrier','eventDTLocal']).dropDuplicates()
		data_footfall.write.mode("append").parquet("s3://staging-near-data-analytics/Adithya/JPN_Tourists_china_korea/{}/JPN_refined_data/{}/".format(currentdate,date))
	except Exception as e:
		print(e)