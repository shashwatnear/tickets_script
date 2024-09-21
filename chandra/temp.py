from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from util import *
from pyspark.sql.functions import row_number
from pyspark.sql.window import *

spark = SparkSession.builder.appName("3916").getOrCreate()

aa = spark.read.parquet("s3://staging-near-data-analytics/Chandrashekhar/postcode_final/ifa_segId_aspkId_april/")
aa  = aa.withColumn("ifa", lower(col("ifa")))

nk = spark.read.csv("s3://near-carbonplatform-data/internal/ifa-tpid-dataset/tenantId=4d5cc3a0/dsId=1178/current/", header = True)

nk = nk.withColumn("tpids",explode(split(col("tpids"),'\\|')))
nk  = nk.withColumn("ifa", lower(col("ifa")))

final = aa.join(nk, nk.ifa == aa.ifa, "inner").drop(nk.ifa)


final1 = final.select('segId', 'ifa', 'tpids').distinct()

window_spec = Window.partitionBy("segId", "ifa").orderBy("tpids")
final1 = final1.withColumn("row_num", row_number().over(window_spec))

#adding cutoffs 2#
window_spec_2 = Window.partitionBy("segId", "ifa")
final2 = final1.withColumn("max_row_num", max('row_num').over(window_spec_2))

final3 = final2.filter(final2.max_row_num <= 8)

#writing the data#
final4 = final3.select("segId", "tpids").distinct()

segId = [85234,85940,87556,87383,87269,86257,87004,87636,85510,86972,87759,86178,86553,85821,87206,85477,86676,87194,87050,86669,87658,85125,87681,87025,87058,87049,86874,86245,85617,87334,85717,86012,87046,86345,87164,87421,87445,87282,85314,85042,87729,86236,87462,85237,87041,86815,87505,86166,86499,85543,86882,87292,87377,85973,87709,85508,87132,86206,86256,85009,86416,85712,85560,86242,85229,86261,86399,86679,87019,85923,87051,87290,85447,85455,86282,87065,87205,85401,87332,85199,85012,87252,86223,87047,85268,87417,86043,87056,85515,86239,85777,87304,87018,86875,86845,85517,87364,87602,86955,87320,85936,87725,85529,87676,85232,85470,86495,85924,86699,85979,86227,86500,85711,87361,87328,85780,85528,85505,87285,86450,86723,86770,85454,86541,87062,87530,85645,87413,86315,87335,86860,86277,87352,87675,86760,85236,87329,85005,85768,87302,86751,85715,87542,86013,86291,86859,86869,86042,87757,86746,87330,86237,87369,86483,86290,85970,87755,85235,85233,86962,85031,85275,85794,87326,87614,85346,87037,87191,86432,85544,85402,87270,86724,86945,85451,86855,86300,85930,86524,85448,85541,87059,87210,86871,85047,85482,87161,87201,87023,87017,87740,87403,86041,85627,86225,86317,85059,85228,87678,86046,85048,87060,85535,87294,86415,86862,86866,85464,87414,85484,85820,87435,86280,86234,87381,87367,86406,87010,86386,85449,85239,87211,86745,86047,86687,86275,87319,86949,87339,85318,85786,86318,85713,84988,85278,86414,86316,87022,86301,85485,86632,87702,86045,86948,85046,85466,86470,85716,87682,87366]

for i in segId:
    final5 = final4.filter(final4.segId == i)
    final5.select("tpids").distinct().repartition(1).write.parquet("s3://staging-near-data-analytics/Chandrashekhar/postcode_final/segments/{}/".format(i))
    print(i)