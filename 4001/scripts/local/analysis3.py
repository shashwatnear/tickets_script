from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# from util import *
from pyspark.sql.functions import create_map, lit, col, lower
from itertools import chain
from thefuzz import fuzz

spark = SparkSession.builder.appName("4001").getOrCreate()

def similarity_score(city1, city2):
    return fuzz.ratio(city1, city2)
similarity_udf = udf(similarity_score, IntegerType())

country_mapping = {
    "FI": "fin",
    "BR": "bra",
    "CZ": "cze",
    "BH": "bhr",
    "DE": "deu",
    "CH": "che",
    "IL": "isr",
    "NG": "nga",
    "SL": "sle",
    "PT": "prt",
    "CA": "can",
    "IR": "irn",
    "PK": "pak",
    "ME": "mne",
    "ZA": "zwe",
    "TG": "tgo",
    "US": "usa",
    "GR": "grc",
    "MC": "mco",
    "QA": "qat",
    "SI": "svn",
    "IE": "irl",
    "IM": "imn",
    "MT": "mlt",
    "RO": "rou",
    "BG": "bgr",
    "SE": "swe",
    "CY": "cyp",
    "HU": "hun",
    "LV": "lva",
    "BE": "bel",
    "DK": "dnk",
    "MM": "mmr",
    "NL": "nld",
    "PF": "pyf",
    "PL": "pol",
    "AT": "aut",
    "PY": "pry",
    "GB": "gbr",
    "GN": "gin",
    "ES": "esp",
    "AE": "are",
    "GI": "gib",
    "SG": "sgp",
    "TN": "tun",
    "IT": "ita",
    "NZ": "nzl",
    "PH": "phl",
    "JE": "jey",
    "LU": "lux",
    "UA": "ukr",
    "CN": "chn",
    "HR": "hrv",
    "GG": "ggy",
    "HK": "hkg",
    "AU": "aus",
    "KZ": "kaz",
    "TR": "tur",
    "FR": "fra",
    "PA": "pan",
    "MA": "mar",
    "NO": "nor",
    "ZW": "zwe"
}
mapping_expr = create_map([lit(x) for x in chain(*country_mapping.items())])

input_ip = spark.read.csv('/Users/nlblr245/Downloads/list3_clean.csv', header = True)

df1 = spark.read.csv('/Users/nlblr245/Downloads/3.csv', header = True).dropDuplicates()
# ip,city_geoname_id,city_name,continent_geoname_id,continent_code,continent_name,country_geoname_id,country_iso_code,country_name,latitude,longitude,accuracy_radius,time_zone,postal_code,subdivision_geoname_id,subdivision_iso_code,subdivision_name,registered_country_geoname_id,registered_country_iso_cod
df2 = spark.read.option("header", "true").option("delimiter", ";").csv('/Users/nlblr245/Downloads/azira_results_3.csv').dropDuplicates()
# ip,pulseplus-country,pulseplus-region,pulseplus-city,pulseplus-conn-speed,pulseplus-conn-type,pulseplus-metro-code,pulseplus-latitude,pulseplus-longitude,pulseplus-postal-code,pulseplus-postal-ext,pulseplus-country-code,pulseplus-region-code,pulseplus-city-code,pulseplus-continent-code,pulseplus-two-letter-country,pulseplus-internal-code,pulseplus-area-codes,pulseplus-country-conf,pulseplus-region-conf,pulseplus-city-conf,pulseplus-postal-conf,pulseplus-gmt-offset,pulseplus-in-dst,pulseplus-timezone-name,homebiz-type,ipc-obs-count,ipc-distinct-device-ids,ipc-distinct-countries,ipc-distinct-regions,ipc-distinct-cities,ipc-distinct-postals,ipc-avg-dist,ipc-max-dist,ipc-dist-std-dev,ipc-last-seen,ipc-radius-weeks-stable,ipc-score,ipc-country-weeks-stable,ipc-region-weeks-stable,ipc-city-weeks-stable,ipc-postal-weeks-stable,host-info


from pyspark.sql.functions import col

# IP Counts
# total_ips = input_ip.count()  # total number of IPs
# df1_ip_count = df1.filter(col('ip').isNotNull()).count()
# df2_ip_count = df2.filter(col('ip').isNotNull()).count()

# df1_ip_coverage = (df1_ip_count / total_ips) * 100
# df2_ip_coverage = (df2_ip_count / total_ips) * 100

# # Country Coverage
# df1_country_count = df1.filter(col('country_name').isNotNull()).count()
# df2_country_count = df2.filter(col('pulseplus-country').isNotNull()).count()

# df1_country_coverage = (df1_country_count / total_ips) * 100
# df2_country_coverage = (df2_country_count / total_ips) * 100

# # State/Region Coverage
# df1_state_count = df1.filter(col('subdivision_name').isNotNull()).count()
# df2_state_count = df2.filter(col('pulseplus-region').isNotNull()).count()

# df1_state_coverage = (df1_state_count / total_ips) * 100
# df2_state_coverage = (df2_state_count / total_ips) * 100

# # City Coverage
# df1_city_count = df1.filter(col('city_name').isNotNull()).count()
# df2_city_count = df2.filter(col('pulseplus-city').isNotNull()).count()

# df1_city_coverage = (df1_city_count / total_ips) * 100
# df2_city_coverage = (df2_city_count / total_ips) * 100

# # Postal Code Coverage
# df1_postal_count = df1.filter(col('postal_code').isNotNull()).count()
# df2_postal_count = df2.filter(col('pulseplus-postal-code').isNotNull()).count()

# df1_postal_coverage = (df1_postal_count / total_ips) * 100
# df2_postal_coverage = (df2_postal_count / total_ips) * 100

# # Show results
# print(f"DF1 IP Count (Non-null): {df1_ip_count}, Coverage: {df1_ip_coverage}%")
# print(f"DF2 IP Count (Non-null): {df2_ip_count}, Coverage: {df2_ip_coverage}%")

# print(f"DF1 Country Mapped (Non-null): {df1_country_count}, Coverage: {df1_country_coverage}%")
# print(f"DF2 Country Mapped (Non-null): {df2_country_count}, Coverage: {df2_country_coverage}%")

# print(f"DF1 States Mapped (Non-null): {df1_state_count}, Coverage: {df1_state_coverage}%")
# print(f"DF2 States Mapped (Non-null): {df2_state_count}, Coverage: {df2_state_coverage}%")

# print(f"DF1 Cities Mapped (Non-null): {df1_city_count}, Coverage: {df1_city_coverage}%")
# print(f"DF2 Cities Mapped (Non-null): {df2_city_count}, Coverage: {df2_city_coverage}%")

# print(f"DF1 Postal Mapped (Non-null): {df1_postal_count}, Coverage: {df1_postal_coverage}%")
# print(f"DF2 Postal Mapped (Non-null): {df2_postal_count}, Coverage: {df2_postal_coverage}%")

# #########################

from pyspark.sql.functions import col

# Convert relevant columns in both DataFrames to lowercase
df1_lower = df1.withColumn('country_name', lower(col('country_name'))) \
               .withColumn('subdivision_name', lower(col('subdivision_name'))) \
               .withColumn('city_name', lower(col('city_name'))) \
               .withColumn('postal_code', lower(col('postal_code'))) \
               .withColumn('subdivision_iso_code', lower('subdivision_iso_code')) \
               .withColumn('country_iso_code', mapping_expr[col('country_iso_code')])



df2_lower = df2.withColumn('pulseplus-country', lower(col('pulseplus-country'))) \
               .withColumn('pulseplus-region', lower(col('pulseplus-region'))) \
               .withColumn('pulseplus-city', lower(col('pulseplus-city'))) \
               .withColumn('pulseplus-postal-code', lower(col('pulseplus-postal-code')))

# Join both DataFrames on the 'ip' column
df_joined = df1_lower.join(df2_lower, on='ip', how='inner').dropDuplicates()

# Compare countries (same, different assignments)
df_countries_same = df_joined.filter(col('country_iso_code') == col('pulseplus-country')).distinct().count()
df_countries_different = df_joined.filter(col('country_iso_code') != col('pulseplus-country')).distinct().count()

# Compare states (same, different assignments)
df_states_same = df_joined.filter(col('subdivision_iso_code') == col('pulseplus-region')).distinct().count()
df_states_different = df_joined.filter(col('subdivision_iso_code') != col('pulseplus-region')).distinct().count()

# Compare cities (same, different assignments)
# df_cities_same = df_joined.filter(col('city_name') == col('pulseplus-city')).distinct().count()
# df_cities_different = df_joined.filter(col('city_name') != col('pulseplus-city')).distinct().count()
df_cities = df_joined.withColumn("similarity_score", similarity_udf(col("city_name"), col("pulseplus-city")))
threshold = 65
df_cities_same = df_cities.filter(col("similarity_score") >= 65).count()
df_cities_different = df_cities.filter(col("similarity_score") < 65).count()

# Compare postal codes (same, different assignments)
df_postals_same = df_joined.filter(col('postal_code') == col('pulseplus-postal-code')).distinct().count()
df_postals_different = df_joined.filter(col('postal_code') != col('pulseplus-postal-code')).distinct().count()

# Show results
print(f"Countries Same Assignment: {df_countries_same}")
print(f"Countries Different Assignment: {df_countries_different}")

print(f"States Same Assignment: {df_states_same}")
print(f"States Different Assignment: {df_states_different}")

print(f"Cities Same Assignment: {df_cities_same}")
print(f"Cities Different Assignment: {df_cities_different}")

print(f"Postals Same Assignment: {df_postals_same}")
print(f"Postals Different Assignment: {df_postals_different}")