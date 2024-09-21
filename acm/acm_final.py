from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from util import *

spark = SparkSession.builder.appName("match_client_promixa").getOrCreate()

maids_promixa = spark.read.json('s3://staging-near-datascience/sumanth/proxima/idcollection/V3/UdJd40JW/*')
# +------------------------------------------------------------------+-------+----------------------------------------------------------------------------+------------------------------------+------+
# |hems                                                              |hl     |maids                                                                       |preferredMaid                       |score |
# +------------------------------------------------------------------+-------+----------------------------------------------------------------------------+------------------------------------+------+
# |[00007302ef78164bb29e1a75a9ea65e971aecce0f4e7a09ff3b496fe27ba4d76]|qd66uvv|[5AA4921B-7301-4C62-9438-F5E2939AB51C]                                      |5AA4921B-7301-4C62-9438-F5E2939AB51C|1.0   |
# |[0001672f8133557a2d041e94cb165a64c5975764fdc27052f22dcdeaf40e57c7]|r1qcp2z|[F2AB4EF0-D6D5-4D12-84EA-8AA3D9D99C40]                                      |F2AB4EF0-D6D5-4D12-84EA-8AA3D9D99C40|1.0   |
# |[00016b93cdc249f1ad7dc9a5c891118958da783cece293224f8a8d3040a1acb3]|r7hgkkk|[8933B459-EF8F-47E4-A755-B4879B8E289E, 73B6F751-864E-4BC3-A768-E9BAA2B00DFB]|8933B459-EF8F-47E4-A755-B4879B8E289E|1.0   |
# |[00022a7f439555d2cc48812c7fe37b41eb9314980729e3849b344bb6b540ecb1]|r3grr01|[417C1E82-1FE0-47EC-9469-AC9182C3965A]                                      |417C1E82-1FE0-47EC-9469-AC9182C3965A|50.0  |
# |[000240ffcd38669cd6ffc53d86e011716198fee0e0e7ade16aed0b048b0de241]|r64b9hd|[741833F7-DCA6-4F18-B96A-FF1499910590]                                      |741833F7-DCA6-4F18-B96A-FF1499910590|1000.0|
# +------------------------------------------------------------------+-------+----------------------------------------------------------------------------+------------------------------------+------+

maids_promixa = maids_promixa.select('maids')
maids_promixa = maids_promixa.withColumn('maids', explode('maids')).dropDuplicates()
maids_promixa.coalesce(1).write.mode("append").csv("s3://staging-near-data-analytics/shashwat/acm/maids/promixa/", header = True)

# maids_cross_matrix = spark.read.parquet('s3://staging-near-data-analytics/shashwat/acm/cross_matrix_90days/*/*')
'''
{
  "ifa": "B1CDF4D9-DCBD-4625-A4EC-0857D05D4B7C",
  "ncid": "053cc26a754f8e0b5a945007e2c59900"
}
'''
# maids_cross_matrix = maids_cross_matrix.select('ifa').dropDuplicates()
# maids_cross_matrix.coalesce(1).write.mode("append").csv("s3://staging-near-data-analytics/shashwat/acm/maids/cross_matrix/", header = True)





