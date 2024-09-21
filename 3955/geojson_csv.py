# Code to convert geojson to set of geohashes. This only applies to a single polygon.
import geojson
import geohashlite   ###  NEEDS TO BE PIP INSTALLED 
import pandas as pd

with open("/Users/nlblr245/Desktop/ticket/3955/Sensitive_FamilyPlanning_single_polygon.json") as f:  ### ADD YOUR GEOJSON PATH HERE
    gj = geojson.load(f)
    
print(gj)

converter_2 = geohashlite.GeoJsonHasher()
converter_2.geojson = gj
converter_2.encode_geojson(precision=8)    ### SPECIFY PRECISION HERE
df = pd.DataFrame(converter_2.geohash_codes, columns = ['geohash'])
df
df.to_csv("/Users/nlblr245/Desktop/ticket/3955/Sensitive_FamilyPlanning_single_polygon_gh8.csv")  #### SPECIFY WRITE PATH HERE
df['geohash'].count()

