#! /usr/bin/env python3
import os
import pickle

import numpy as np
import pandas as pd

from cluster import ATTR, get_numeric_data

ONLY_NEW = False


def main():
    with open("cluster_centers.pkl", "rb") as f:
        km = pickle.load(f)
    centers = getattr(km, ATTR)
    centers = zip(range(len(centers)), centers)
    centers = sorted(centers, key=lambda x: np.linalg.norm(x[1]))
    label2rank = dict(zip([c[0] for c in centers], range(len(centers))))

    for normalized_fn in os.listdir("normalized_data"):
        normalized_fn = os.path.join("normalized_data", normalized_fn)
        labeled_fn = normalized_fn.replace("normalized_data", "labeled_data")
        if ONLY_NEW and os.path.exists(labeled_fn):
            continue
        print("Labeling", labeled_fn)
        data = get_numeric_data(normalized_fn)
        data.dropna(inplace=True)
        labels = km.predict(data)
        data = pd.read_pickle(normalized_fn)
        data = pd.concat(
            [
                data,
                pd.Series(labels, name="RANK").map(lambda x: label2rank[x]),
            ],
            axis=1,
        )
        data.index = data["EVENT_ID"]
        data = data.drop(columns="EVENT_ID")
        data.to_pickle(labeled_fn)

    fips_ranksum = {}
    for filename in os.listdir("labeled_data"):
        # year = (int(filename.split(".")[0]) - 1950) / (2020 - 1950)
        filename = os.path.join("labeled_data", filename)
        data = pd.read_pickle(filename)
        data = data[["FIPS", "RANK"]]
        data = data.groupby("FIPS").sum()
        for fip in data.index:
            fips_ranksum.setdefault(fip, 0)
            fips_ranksum[fip] += data.loc[fip, "RANK"]
            # fips_ranksum[fip] += data.loc[fip, "RANK"] * year

    data = pd.DataFrame(fips_ranksum.items(), columns=["FIPS", "SEVERITY"])
    print(data.describe())
    data.to_pickle("fips_severity.pkl")


if __name__ == "__main__":
    main()
