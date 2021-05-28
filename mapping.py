#! /usr/bin/env python3
import json

import branca
import folium
import pandas as pd
import requests


def main():
    def style_function(feature):
        severity = geo_data.get(feature["id"], None)
        return {
            "fillOpacity": 0.8,
            "weight": 0,
            "fillColor": "#555555"
            if severity is None
            else colorscale(severity),
        }

    url = "https://raw.githubusercontent.com/python-visualization/folium/master/examples/data"
    county_geo = f"{url}/us_counties_20m_topo.json"

    geo_data = pd.read_pickle("fips_severity.pkl")
    geo_data["FIPS"] = geo_data["FIPS"].map(lambda x: "0500000US{}".format(x))

    geo_data = geo_data.set_index("FIPS")["SEVERITY"]
    geo_data = geo_data.dropna()
    colorscale = branca.colormap.LinearColormap(
        colors=["white", "red"], vmin=geo_data.min(), vmax=geo_data.max()
    )

    m = folium.Map(location=[39, -98], tiles="cartodbpositron", zoom_start=5)

    folium.TopoJson(
        json.loads(requests.get(county_geo).text),
        "objects.us_counties_20m",
        style_function=style_function,
    ).add_to(m)

    m.save("map.html")


if __name__ == "__main__":
    main()
