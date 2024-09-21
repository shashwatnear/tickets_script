from thefuzz import fuzz
from thefuzz import process
import pandas as pd

# test
# name = "Kurtis Pykes"
# full_name = "Kurtis K D Pykes"

# print(f"Similarity score: {fuzz.ratio(name, full_name)}")

df1 = pd.read_csv('/Users/nlblr245/Desktop/ticket/fuzzy_string/maxmind.csv', header=0)
df2 = pd.read_csv('/Users/nlblr245/Desktop/ticket/fuzzy_string/digital_elements.csv', header=0)

df3 = pd.merge(df1, df2, on='ip')
print(df3)

def similarity_score(city1, city2):
    return fuzz.ratio(city1.lower(), city2.lower())

df3['similarity_score'] = df3.apply(lambda row: similarity_score(row['city_name'], row['pulseplus-city']), axis=1)

threshold = 60

filtered_df = df3[df3['similarity_score'] >= threshold]

print(filtered_df[['ip', 'city_name', 'pulseplus-city', 'similarity_score']])