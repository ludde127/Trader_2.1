from pprint import pprint
from __init__ import files, read_yaml
import requests
from time import sleep
import pandas as pd

APIKEY = read_yaml(files["auth"])["OpenWeatherMapKey"]
print(APIKEY)
city_id = 5128581
r = requests.get(f'http://api.openweathermap.org/data/2.5/weather?id={city_id}&units=metric&appid={APIKEY}')
pprint(r.json())

city_list = pd.read_json(files["city_list"])
name = "New York City"
city_list.set_index("id")

""" WORKS BUT SLOW AS FUCK
for row in city_list.iterrows():
    n = row[1]["name"].lower()
    if str(n) == name.lower():
        id = row[1]["id"]
        print(id)

"""

row = city_list.loc[city_list["name"] == name]
id = row.id
print(id)