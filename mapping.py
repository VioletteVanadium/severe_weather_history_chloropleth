#! /usr/bin/env python3
import json
from urllib.request import urlopen

import branca
import fiona
import folium
import numpy as np
import pandas as pd
import plotly.express as px
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon

NORM_AREA = True


def main():
    def style_function(feature):
        try:
            severity = float(geo_data[feature["id"]])
            color = colorscale(severity)
        except KeyError:
            color = "#ffffff"
        return {
            "fillOpacity": 0.8,
            "weight": 0,
            "fillColor": color,
        }

    with urlopen(
        "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
    ) as response:
        counties = json.load(response)

    geo_data = pd.read_pickle("fips_severity.pkl")
    geo_data = geo_data.set_index("FIPS")["SEVERITY"]

    if NORM_AREA:
        area_data = pd.read_pickle("fips_area.pkl")
        geo_data = geo_data.divide(area_data)
        with pd.option_context("mode.use_inf_as_na", True):
            geo_data = geo_data.dropna()

    geo_data = geo_data.map(lambda x: (x - geo_data.mean()) / geo_data.std())
    geo_data = pd.DataFrame(
        {"FIPS": geo_data.index, "SEVERITY": geo_data.values}
    )
    print(geo_data.describe())

    fig = px.choropleth(
        geo_data,
        geojson=counties,
        locations="FIPS",
        color="SEVERITY",
        scope="usa",
        range_color=(-1, 1),
    )
    fig.show()


if __name__ == "__main__":
    main()
