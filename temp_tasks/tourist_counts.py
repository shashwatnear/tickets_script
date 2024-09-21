from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.sql import SparkSession
import requests,json
import proximityhash
import Geohash
import time
import subprocess
from util import *
import boto3
import pandas as pd
import datetime
from datetime import datetime,timedelta

spark = SparkSession.builder.appName("Singapore - Tourist cards refresh").getOrCreate()
sc = spark.sparkContext

segment_ids = [90830,90831,90828,90829,90827,90826,90825,90824,90823,90822,90821,90820,90818,90819,90817,90816,90815,90814,90812,90813,90810,90811,90809,90808]

for segment_id in segment_ids:
    df = spark.read.csv(f's3://staging-near-data-analytics/tourist_dags_data/sgp/Tourist_card/2024-08-04/Tourist_card_seg/segment_id={segment_id}/*')
    print("Segment ID", segment_id)
    print(df.distinct().count())