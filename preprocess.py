#! /usr/bin/env python3
import argparse
import datetime
import gzip
import os
import re
import resource
import time
from collections.abc import Callable
from ftplib import FTP
from multiprocessing import Pool
from queue import Queue
from typing import Any, Union

import geopy
import numpy as np
import pandas as pd

GET_COLUMNS = [
    "EPISODE_ID",
    "EVENT_ID",
    "EVENT_TYPE",
    "BEGIN_YEARMONTH",
    "BEGIN_DAY",
    "BEGIN_TIME",
    "END_YEARMONTH",
    "END_DAY",
    "END_TIME",
    "CZ_TYPE",
    "STATE_FIPS",
    "CZ_FIPS",
    "STATE",
    "CZ_NAME",
    "INJURIES_DIRECT",
    "INJURIES_INDIRECT",
    "DEATHS_DIRECT",
    "DEATHS_INDIRECT",
    "DAMAGE_PROPERTY",
    "DAMAGE_CROPS",
    "MAGNITUDE",
    "MAGNITUDE_TYPE",
    "TOR_F_SCALE",
    "TOR_LENGTH",
    "TOR_WIDTH",
    "BEGIN_LAT",
    "BEGIN_LON",
    "END_LAT",
    "END_LON",
]
NORMALIZE_COLUMNS = [
    "INJURIES_DIRECT",
    "INJURIES_INDIRECT",
    "DEATHS_DIRECT",
    "DEATHS_INDIRECT",
    "DAMAGE_PROPERTY",
    "DAMAGE_CROPS",
    "WIND_SPEED",
    "HAIL_SIZE",
    "TOR_F_SCALE",
    "TOR_LENGTH",
    "TOR_WIDTH",
    "DURATION",
]

YEAR_PATT = re.compile("_d(\d{4})_")
LOC_DICT = {}


def main(args: argparse.Namespace) -> None:
    def thread_main():
        t, year = threads.get()
        data = t.get()
        mean, var = get_mean_var(data)
        stats.append((year, mean, var))

    if args.start_year:
        years = [str(year) for year in range(args.start_year, 2021)]
    else:
        years = [str(year) for year in range(1950, 2021)]
    download(years, force=args.download)

    # initial cleaning and column selection
    stats = []
    PROCS = 8
    threads = Queue(maxsize=PROCS)
    with Pool(processes=PROCS, initializer=cpu_limit) as pool:
        for year in years:
            t = pool.apply_async(clean_pickle, (year, args.clean))
            threads.put((t, year))
            if threads.full():
                thread_main()
        while not threads.empty():
            thread_main()

    # determine mean/variance for all years
    means = pd.DataFrame()
    variances = pd.DataFrame()
    for year, mean, var in stats:
        mean.name = year
        var.name = year
        means = pd.concat([means, mean], axis=1)
        variances = pd.concat([variances, var], axis=1)
    mean = means.mean(axis=1)
    var = variances.mean(axis=1) + means.var(axis=1)

    # normalize each year's data set
    for year in years:
        normalize(year, mean, var)


def download(years, force=False):
    with FTP("ftp.ncdc.noaa.gov") as ftp:
        ftp.login()
        ftp.cwd("pub/data/swdi/stormevents/csvfiles/")
        file_list = []
        ftp.dir(file_list.append)
        for filename in file_list:
            filename = filename.split()[-1]
            if "details" not in filename:
                continue
            if os.path.join("data", filename) in os.listdir():
                continue
            date_match = YEAR_PATT.search(filename)
            if not date_match:
                continue
            if not date_match.group(1) in years:
                continue
            save_to = os.path.join("data", filename)
            if not force and os.path.exists(save_to):
                continue
            print("Downloading", filename)
            with open(save_to, "wb") as f:
                ftp.retrbinary("RETR " + filename, f.write)


def clean_pickle(year, clean=False):
    pickle_name = "clean_data/{}.pkl".format(year)
    if clean or not os.path.exists(pickle_name):
        data = get_data(year)
        print("Saving", pickle_name)
        data.to_pickle(pickle_name)
    else:
        data = pd.read_pickle(pickle_name)
    return data


def get_data(year: Union[str, int]) -> pd.DataFrame:
    """
    Get all data in `./data` if year=None, or data for a specific year
    """

    def map_f_scale(item):
        fscale_patt = re.compile("(\d)")
        match = fscale_patt.search(str(item))
        if match:
            return int(match.group(1))
        return np.nan

    def map_damage(item):
        item = (
            str(item)
            .upper()
            .replace("?", "")
            .replace("T", "K" * 4)
            .replace("B", "K" * 3)
            .replace("M", "K" * 3)
            .replace("K", "000")
            .replace("H", "00")
        )
        try:
            item = float(item)
        except ValueError:
            item = np.nan
        return item

    def split_mag(row):
        # split magnitude into wind speeds and hail sizes
        mag_type = row["MAGNITUDE_TYPE"]
        mag = row["MAGNITUDE"]
        if mag_type:
            row["WIND_SPEED"] = mag
            row["HAIL_SIZE"] = 0.0
        else:
            row["WIND_SPEED"] = 0.0
            row["HAIL_SIZE"] = mag
        return row

    for filename in os.listdir("data"):
        filename = os.path.join("data", filename)
        if year is not None and YEAR_PATT.search(filename).group(1) == str(
            year
        ):
            break
    print("Cleaning", filename)
    with gzip.open(filename) as f:
        data = pd.read_csv(f, usecols=GET_COLUMNS, low_memory=False)
    # combine lat/lng into single column
    data = join_columns(
        data,
        lambda x, y: "{},{}".format(x, y),
        ("BEGIN_LOC", ["BEGIN_LAT", "BEGIN_LON"]),
        ("END_LOC", ["END_LAT", "END_LON"]),
    )
    # combine date/time related columns into single datetime column
    data = join_columns(
        data,
        lambda x, y, z: "{}{}:{}".format(
            str(x).rjust(6, "0"), str(y).rjust(2, "0"), str(z).rjust(4, "0")
        ),
        ("BEGIN_DATE_TIME", ["BEGIN_YEARMONTH", "BEGIN_DAY", "BEGIN_TIME"]),
        ("END_DATE_TIME", ["END_YEARMONTH", "END_DAY", "END_TIME"]),
    )
    # format datetime columns as such
    date_fmt = "%Y%m%d:%H%M"
    data["BEGIN_DATE_TIME"] = pd.to_datetime(
        data["BEGIN_DATE_TIME"], format=date_fmt
    )
    data["END_DATE_TIME"] = pd.to_datetime(
        data["END_DATE_TIME"], format=date_fmt
    )
    # calculate duration
    data = join_columns(
        data,
        lambda x, y: (y - x).total_seconds() / 3600,
        ("DURATION", ["BEGIN_DATE_TIME", "END_DATE_TIME"]),
        keep_old=True,
    )
    # exclude maritime event types
    data = data[data["CZ_TYPE"] != "M"]
    # combine type and fips to single column
    data = join_columns(
        data,
        lambda y, z: "{:02d}{:03d}".format(int(y), int(z)),
        ("FIPS", ["STATE_FIPS", "CZ_FIPS"]),
    )
    # combine city/state into same column
    data = join_columns(
        data,
        lambda x, y: "{}, {}".format(x, y),
        ("LOC_NAME", ["CZ_NAME", "STATE"]),
    )
    # fill NaN with 0 for columns that it makes sense for
    for col in [
        "MAGNITUDE",
        "DAMAGE_PROPERTY",
        "DAMAGE_CROPS",
        "TOR_LENGTH",
        "TOR_WIDTH",
    ]:
        data[col] = data[col].fillna(0)
    # remove "EF" or "F" from tornado scale entries
    data["TOR_F_SCALE"] = data["TOR_F_SCALE"].map(map_f_scale)
    # convert K/M/B suffixes to pure numbers
    for col in ["DAMAGE_PROPERTY", "DAMAGE_CROPS"]:
        data[col] = data[col].map(map_damage)
    # split MAGNITUDE according to MAGNITUDE_TYPE
    data = data.apply(split_mag, axis=1)
    # remove unneeded columns
    data = data.drop(columns=["MAGNITUDE", "MAGNITUDE_TYPE"])
    return data


def normalize(year: int, mean: pd.Series, var: pd.Series) -> pd.DataFrame:
    data = clean_pickle(year)
    pickle_name = "normalized_data/{}.pkl".format(year)
    print("Normalizing {} data...".format(year), end=" ")
    for col in var.index:
        data[col] = data[col].map(
            lambda x: x / np.sqrt(var[col]) if var[col] != 0 else 0
        )
    print("Saving", pickle_name)
    data.to_pickle(pickle_name)


def cpu_limit():
    pid = os.getpid()
    os.popen("cpulimit --limit=75 --pid={}".format(pid))


def join_columns(
    data: pd.DataFrame,
    func: Callable[..., Any],
    *args: tuple[str, list[str]],
    keep_old: bool = False,
) -> pd.DataFrame:
    for column_spec in args:
        new_column, old_columns = column_spec
        data[new_column] = list(
            map(
                func,
                *[data[col] for col in old_columns],
            )
        )
        if not keep_old:
            data = data.drop(columns=old_columns)
    return data


def get_mean_var(data: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    pd.set_option("display.max_columns", None)
    # print(data[NORMALIZE_COLUMNS].describe())
    mean = data[NORMALIZE_COLUMNS].mean()
    mean.name = "MEAN"
    var = data[NORMALIZE_COLUMNS].var()
    var.name = "VAR"
    return mean, var


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--download", action="store_true", help="Download csv.gz files"
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Re-clean data and save as pickle",
    )
    parser.add_argument(
        "--start_year", type=int, help="start cleaning at year YEAR"
    )
    args = parser.parse_args()

    main(args)
