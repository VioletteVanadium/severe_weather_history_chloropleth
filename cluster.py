#! /usr/bin/env python3
import os
import pickle

import numpy as np
import pandas as pd
import sklearn.cluster as sk_cluster
from sklearn.model_selection import train_test_split

from preprocess import NORMALIZE_COLUMNS

pd.set_option("display.max_columns", None)
EVENT_TYPES = [
    # "Lakeshore Flood",
    # "Astronomical Low Tide",
    # "Lightning",
    "Avalanche",
    "Blizzard",
    # "Coastal Flood",
    # "Cold/Wind Chill",
    # "Debris Flow",
    # "Rip Current",
    # "Dense Fog",
    # "Seiche",
    # "Dense Smoke",
    "Sleet",
    "Drought",
    "Storm Surge/Tide",
    # "Dust Devil",
    "Strong Wind",
    "Dust Storm",
    "Thunderstorm Wind",
    "Excessive Heat",
    "Tornado",
    "Extreme Cold/Wind Chill",
    "Tropical Depression",
    "Flash Flood",
    "Tropical Storm",
    # "Flood",
    # "Tsunami",
    # "Freezing Fog",
    # "Volcanic Ash",
    # "Frost/Freeze",
    # "Funnel Cloud",
    "Wildfire",
    "Hail",
    "Winter Storm",
    # "Heat",
    # "Winter Weather",
    "Heavy Rain",
    "Heavy Snow",
    # "High Surf",
    "High Wind",
    "Hurricane (Typhoon)",
    "Ice Storm",
    "Lake-Effect Snow",
]
OTHER_COLUMNS = [
    "EPISODE_ID",
    "CZ_TYPE",
    "TOR_F_SCALE",
    "BEGIN_LOC",
    "END_LOC",
    "BEGIN_DATE_TIME",
    "END_DATE_TIME",
    "LOC_NAME",
]
NORMALIZE_COLUMNS.remove("TOR_F_SCALE")
NUMERIC_COLUMNS = NORMALIZE_COLUMNS + EVENT_TYPES

CLUSTER_METHOD = sk_cluster.MiniBatchKMeans
KWARGS = {"n_clusters": 4}
ATTR = "cluster_centers_"
TRAIN_PCT = 0.8


def main():
    centers = None
    for i in range(5):
        print("Iter", i)
        if centers is None:
            centers = pd.DataFrame(each_year())
        else:
            centers = centers.append(
                pd.DataFrame(each_year()), ignore_index=True
            )
    with open("cluster_centers.pkl", "wb") as f:
        pickle.dump(cluster(centers, as_obj=True), f)


def each_year():
    centers = None
    for filename in os.listdir("normalized_data"):
        filename = os.path.join("normalized_data", filename)
        data = get_numeric_data(filename)
        data.dropna(inplace=True)
        if TRAIN_PCT < 1:
            data, _ = train_test_split(data, train_size=TRAIN_PCT)
        if centers is None:
            centers = pd.DataFrame(cluster(data))
        else:
            centers = centers.append(
                pd.DataFrame(cluster(data)), ignore_index=True
            )
    return centers


def get_numeric_data(filename):
    data = pd.read_pickle(filename)
    data = pd.get_dummies(
        data, columns=["EVENT_TYPE"], prefix=[""], prefix_sep=""
    )
    for et in EVENT_TYPES:
        if et not in data.columns:
            data = pd.concat(
                [
                    data,
                    pd.Series([0] * len(data), name=et, index=data.index),
                ],
                axis=1,
            )
    data.index = data["EVENT_ID"]
    data = data.drop(columns="EVENT_ID")
    data = data[NUMERIC_COLUMNS]
    return data


def cluster(data, as_obj=False):
    km = CLUSTER_METHOD(**KWARGS)
    res = km.fit(data)
    if as_obj:
        return res
    return getattr(res, ATTR)


if __name__ == "__main__":
    main()
