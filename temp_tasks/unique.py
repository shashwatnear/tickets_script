from pyspark.sql.functions import *
from datetime import date,timedelta,datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col,split
import datetime
from datetime import datetime, timedelta
import requests,json
import pandas as pd
import proximityhash
import Geohash
import pyspark.sql.functions as f
from util import *
import argparse

spark = SparkSession.builder.appName("adithya").getOrCreate()
sc = spark.sparkContext

def do_it(file1, file2, folder_name):
	df1 = spark.read.csv(file1)
	df2 = spark.read.csv(file2)
	df3 = df1.union(df2)
	df3 = df3.dropDuplicates()
	df3.coalesce(1).mode('append').csv(f"s3://staging-near-data-analytics/manoj/{folder_name}/")
	return "SUCCESS"


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Enter date format like YYYY-MM-DD')
	parser.add_argument('--file1', type=str, help='s3 path', required=True)
	parser.add_argument('--file2', type=str, help='s3 path', required=True)
	parser.add_argument('--folder_name', type=str, help='Enter folder name to save the final file', required=True)
	args = parser.parse_args()
	file1 = args.file1
	file2 = args.file2
	folder_name = args.folder_name

	result = do_it(file1, file2, folder_name)
	print(result)
	print("File can be found in path: ")
	print(f"s3://staging-near-data-analytics/manoj/{folder_name}/")





