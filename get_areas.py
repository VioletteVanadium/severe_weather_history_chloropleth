#! /usr/bin/env python3
import pandas as pd
import requests
from bs4 import BeautifulSoup

from preprocess import join_columns

base_url = "https://tigerweb.geo.census.gov/tigerwebmain/"
res = requests.get(base_url + "TIGERweb_tract_current.html")
soup = BeautifulSoup(res.content, features="lxml")

all_data = pd.Series(dtype="int")
count = 0
for state in soup.find_all("td"):
    count += 1
    link = state.find("a").attrs["href"]
    name = " ".join(state.find("a").text.split())
    try:
        data = pd.read_html(base_url + link)[0]
    except ValueError:
        print(link)
        continue
    join_columns(
        data,
        lambda x, y: "{:02}{:03}".format(x, y),
        ("FIPS", ["STATE", "COUNTY"]),
    )
    join_columns(
        data,
        lambda x, y: x + y,
        ("AREA", ["AREALAND", "AREAWATER"]),
    )
    data = data[["FIPS", "AREA"]]
    data = data.groupby("FIPS").sum()
    data = data["AREA"]
    all_data = all_data.append(data)

all_data.to_pickle("fips_area.pkl")
