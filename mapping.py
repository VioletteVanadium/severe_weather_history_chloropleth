#! /usr/bin/env python3
import json

import branca
import fiona
import folium
import numpy as np
import pandas as pd
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon


def main():
    def style_function(feature):
        try:
            severity = float(geo_data[feature["id"]])
        except KeyError:
            severity = 0
        color = colorscale(severity)
        return {
            "fillOpacity": 0.8,
            "weight": 0,
            "fillColor": color,
        }

    geo_data = pd.read_pickle("fips_severity.pkl")
    geo_data["FIPS"] = geo_data["FIPS"].map(lambda x: "0500000US{}".format(x))
    geo_data = geo_data.set_index("FIPS")["SEVERITY"]

    area_data = get_areas()
    fill_val = area_data.mean()
    geo_data = pd.concat([geo_data, area_data], axis=1)
    geo_data = geo_data.apply(
        lambda x: pd.Series(
            [
                (x["SEVERITY"] / x["AREA"])
                if not pd.isna(x["AREA"])
                else (x["SEVERITY"] / fill_val)
            ],
        ),
        axis=1,
    )
    geo_data.columns = ["SEVERITY"]
    geo_data = geo_data["SEVERITY"]
    geo_data = geo_data.dropna()

    colorscale = branca.colormap.LinearColormap(
        colors=["white", "red"], vmin=geo_data.min(), vmax=geo_data.max()
    )

    m = folium.Map(location=[39, -98], zoom_start=5)

    with open("topo_json_us_counties.json") as f:
        topo_str = f.read()
    topo = folium.TopoJson(
        json.loads(topo_str),
        "objects.us_counties_20m",
        style_function=style_function,
    )
    topo.add_to(m)

    m.save("map.html")


def get_areas():
    shape = fiona.open("topo_json_us_counties-polygon.shp")
    data = []
    for s in shape.items():
        fips = s[1]["properties"]["id"]
        geom = s[1]["geometry"]
        polygons = []
        for tmp in geom["coordinates"]:
            try:
                polygons.append(Polygon(tmp))
            except ValueError:
                polygons.append(Polygon(tmp[0]))
        polygon = MultiPolygon(polygons)
        area = polygon.area
        data.append((fips, np.abs(area)))
    data.sort(key=lambda x: x[0])
    data = pd.DataFrame(data)
    data.columns = ["FIPS", "AREA"]
    data = data.set_index("FIPS")["AREA"]
    return data


if __name__ == "__main__":
    main()
