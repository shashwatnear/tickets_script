import pandas as pd
import maxminddb
db = maxminddb.open_database("s3://staging-near-data-analytics/shashwat/ps-3936/files/GeoIP2-City.mmdb")   
df = pd.read_csv('s3://staging-near-data-analytics/shashwat/ps-3936/ip_list/part-00000-61bff777-1b36-4832-91c9-989a89a1f7a2-c000.csv')
df.head(3)
df = df.rename(columns = {'_c0' : 'ip'}, inplace = False)
df_lis = df['ip'].to_list()

dfg = pd.DataFrame(columns = ["ip", "maxmind_value"])

for i in range(len(df_lis)):
	rs = db.get(df_lis[i])
	dfg.loc[i] =[df_lis[i],rs]

dfg.head(3)

# paths:
# s3://near-compass-prod-data/compass/measurement/version=1.0.0/trigger_id=65e22c0e29ba7472dcf64eb2/data_cache/impressions/