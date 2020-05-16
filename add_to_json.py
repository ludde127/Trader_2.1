import pandas as pd
from __init__ import files
"""
file = files["city_list"]
json_data = pd.read_json(file)
json_data_two = pd.read_json("facts/country_codes.json")
new_df = pd.DataFrame
for row in json_data.iterrows():
    country = row["country"]

def one():
    file = files["city_list"]
    json_data = pd.read_json(file)

    search_key = "name"
    search_query = "Osby"
    print(json_data)
    row = json_data.loc[json_data[search_key] == search_query]

    print(row)

def two(file):
    json_data = pd.read_json(file)
    print(json_data)

two("facts/country_codes.json")
"""

data = pd.read_csv("facts/city_gdp.csv")
df_new = pd.DataFrame(columns=data.columns.add("gdp_avg"))
cols_to_average = "official,Brookings,PwC,McKinsey,Other"
weights = [40, 30, 15, 10, 5]
for row in data.iterrows():
    i = 0
    for gdp in cols_to_average:
        if type(gdp) is list and gdp > 0:
        gdps += gdp
        i += 0
    row["gdp_avg"] =


print(data)
print(df_new)