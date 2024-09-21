from datetime import date,timedelta
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_date
from pyspark.sql.window import Window as W
from pyspark.sql.functions import *
from util import *
import proximityhash
import hashlib
import argparse

spark = SparkSession.builder.appName("3932_refined").getOrCreate()

# UDFs
def get_geohash(lat, lon):
    a = proximityhash.create_geohash(float(lat), float(lon), 50, 8)
    return a
generate_geohash_udf = udf(get_geohash)

def computeHash(ID):
    m = hashlib.sha1((ID.upper() + "9o8WnUtwdY").encode())
    return m.hexdigest()

sha_udf = udf(computeHash,StringType())

# -------------------------------------------------------------

def getPoiFootfall(file_path, dates, hours):
    # poi
    poi = spark.read.csv(file_path, header = True)
    poi = poi.withColumn("geohash", generate_geohash_udf(col('lat'), col('lon')))
    poi = poi.withColumn('geohash',(split(poi['geohash'],',')))
    poi = poi.withColumn('geohash',explode('geohash'))

    # Footfall report
    for date in dates:
        for hour in hours:
            try:
                day = "{:02d}".format(date.day)
                month = '{:02d}'.format(date.month)
                year = date.year
                dest_path = "s3://staging-near-data-analytics/shashwat/ps-3932/refined/footfall/{}/{}/".format(date, hour)

                data = spark.read.parquet(f"s3://near-data-warehouse/refined/dataPartner=*/year={year}/month={month}/day={day}/hour={hour}/country={country}/*")
                data = data.select(['ifa', data.geoHash9.substr(1, 8).alias('geohash'), 'coreSeg'])
                ff_df = data.join(poi, on='geohash', how='inner').drop('geohash')
                ff_df.write.mode("overwrite").parquet(dest_path)
            except:
                print(date, " Path does not exists")
    return "SUCCESS"

# -------------------------------------------------------------

# report 1 | device_id, profile
def getReport1():
    df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/refined/footfall/*/*/*')
    df = df.withColumn('ifa', upper('ifa'))
    demog_df = spark.read.csv('s3://near-data-analytics/Ajay/coreSeg.csv', header=True)

    result = df.select(['ifa','coreSeg']).dropDuplicates()
    result = result.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
    result = result.withColumn('coreSeg', explode_outer('coreSeg')).drop_duplicates()
    result = result.join(broadcast(demog_df), on='coreSeg').drop('coreSeg')
    result = result.withColumnRenamed('desc', 'Profile')
    result = result.select(['deviceID', 'Profile'])

    # now profiles for 1st report
    age = result.filter((col('Profile') == '18-24') | (col('Profile') == '25-34') | (col('Profile') == '35-44') | (col('Profile') == '45-54') | (col('Profile') == '55+'))
    gender = result.filter((col('Profile') == 'Male') | (col('Profile') == 'Female'))
    profiles = result.filter((col('Profile') == 'Affluents') | (col('Profile') == 'Students') | (col('Profile') == 'Shoppers') | (col('Profile') == 'Parents') | (col('Profile') == 'Professionals'))

    # early retirement
    pro = result.filter(col('Profile') == 'Professionals')
    ff = result.filter(col('Profile') == '45-54')
    early_retirement = pro.join(ff, on = 'deviceID', how = 'inner')
    early_retirement = early_retirement.select('deviceID').withColumn('Profile', lit('Early_Retirement'))

    # retirement
    fifty_five_plus = result.filter(col('Profile') == '55+')
    retirement = pro.join(fifty_five_plus, on = 'deviceID', how = 'inner')
    retirement = retirement.select(['deviceID']).withColumn('Profile', lit('Retirement_State'))

    final = age.union(gender).union(profiles).union(early_retirement).union(retirement)
    # result.write.mode('overwrite').parquet('s3://staging-near-data-analytics/shashwat/ps-3932/refined/report1/')
    final.coalesce(1).write.mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-3932/reports/refined/report1/", header=True)
    return "SUCCESS"


# -------------------------------------------------------------

# report 2 | device_id, gender, age
def getReport2():
    df = spark.read.csv('s3://staging-near-data-analytics/shashwat/ps-3932/reports/refined/report1/', header = True)

    gender = df.filter((col('Profile') == 'Male') | (col('Profile') == 'Female'))
    gender = gender.withColumnRenamed('Profile', 'Gender')

    age = df.filter((col('Profile') == '18-24') | (col('Profile') == '25-34') | (col('Profile') == '35-44') | (col('Profile') == '45-54') | (col('Profile') == '55+'))
    age = age.withColumnRenamed('Profile', 'Age')

    result = gender.join(age, on = 'deviceID', how = 'inner')
    result.coalesce(1).write.mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-3932/reports/refined/report2/", header=True)
    return "SUCCESS"
# -------------------------------------------------------------

# report 3 | deviceID, Category, Sub-Category, Location, Lat, Lon, Month, Footfall
def getReport3(dates, hours):
    for date in dates:
        for hour in hours:
            df = spark.read.parquet("s3://staging-near-data-analytics/shashwat/ps-3932/refined/footfall/{}/{}/".format(date, hour))
            df = df.withColumn('date', lit('date')).withColumn('hour', lit('hour'))
            grouped_df = df.groupBy('ifa', 'date', 'hour').agg(countDistinct('ifa').alias('ff'))
            footfall_df = grouped_df.groupBy('ifa').agg(sum('ff').alias('daily_footfall'))
            footfall_df.write.mode('overwrite').parquet(f's3://staging-near-data-analytics/shashwat/ps-3932/refined/daily_footfall/{date}/')

    # Total Footfall
    df = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/refined/daily_footfall/*/*')
    df = df.groupBy('ifa').agg(sum('daily_footfall').alias('total_footfall'))
    df.write.mode('overwrite').parquet(f's3://staging-near-data-analytics/shashwat/ps-3932/refined/total_footfall/')

    df1 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/refined/footfall/*/*')
    df2 = spark.read.parquet('s3://staging-near-data-analytics/shashwat/ps-3932/refined/total_footfall/*')
    result = df1.join(df2, on = 'ifa', how = 'inner').dropDuplicates()
    # result = result.withColumn('Month', date_format(current_date(), "MMMM")).dropDuplicates()
    result = result.withColumn('Month', lit('May'))
    result = result.select(['ifa', 'category', 'subCategory', 'location', 'lat', 'lon', 'Month', 'total_footfall'])
    result = result.withColumn('deviceID', sha_udf(col('ifa'))).drop('ifa')
    result = result.withColumnRenamed('total_footfall', 'Footfall')
    result.coalesce(1).write.mode('overwrite').csv("s3://staging-near-data-analytics/shashwat/ps-3932/reports/refined/report3/", header=True)
    return "SUCCESS"


# -------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Enter date format like YYYY-MM-DD')
    parser.add_argument('--file_path', type=str, help='s3://staging-near-data-analytics/shashwat/ps-3932/files/SMRT-Dashboard location master v1 - Sheet1.csv', required=True)
    parser.add_argument('--month', type=str, help='May', required=True)

    args = parser.parse_args()
    file_path = args.file_path
    month = args.month

    start = "2024-05-01"
    end = "2024-05-31"
    country = "SGP"

    dates = get_dates_between(start, end)
    hours = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"]

    footfall = getPoiFootfall(file_path, dates, hours)
    report1 = getReport1()
    report2 = getReport2()
    report3 = getReport3(file_path, month, dates, hours)