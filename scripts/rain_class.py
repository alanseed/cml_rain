# Classify the raining periods using the RAINLINK algorithm as a guide
import pandas as pd
from pymongo import MongoClient
import numpy as np
import pymongo
import argparse
import pathlib
import os
import sys
import pymongo.collection
from datetime import datetime


def valid_date(s: str) -> np.datetime64:
    """
    Validate and parse a date string.

    Args:
        s (str): The date string to validate.

    Returns:
        np.datetime64: The parsed datetime object.

    Raises:
        argparse.ArgumentTypeError: If the date string is not valid.
    """
    try:
        return np.datetime64(s)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Not a valid date: {s!r}") from e


def get_cmls(
    cml_col: pymongo.collection.Collection,
    longitude: float,
    latitude: float,
    max_range: float,
) -> pd.DataFrame:
    """Return the CMLs that are within a radius of a location

    Args:
        cml_col (pymongo.collection.Collection): Collection of CMLs
        longitude (float): degrees of longitude
        latitude (float): degress of latitude
        max_range (float): maximum range in m

    Returns:
        pd.DataFrame:
    """
    query = {
        "properties.midpoint": {
            "$nearSphere": {
                "$geometry": {"type": "Point", "coordinates": [longitude, latitude]},
                "$maxDistance": max_range,
            }
        }
    }

    records = []
    for doc in cml_col.find(filter=query):
        record = {
            "link_id": doc["properties"]["link_id"],
            "frequency": float(doc["properties"]["frequency"]["value"]),
            "length": float(doc["properties"]["length"]["value"]),
            "mid_lon": float(doc["properties"]["midpoint"]["coordinates"][0]),
            "mid_lat": float(doc["properties"]["midpoint"]["coordinates"][1]),
        }
        records.append(record)

    # Convert the list of records to a DataFrame
    cml_df = pd.DataFrame(records)
    return cml_df


def cl_rain(
    cmls: pd.DataFrame,
    cml_col: pymongo.collection.Collection,
    data_col: pymongo.collection.Collection,
    start_time: np.datetime64,
    end_time: np.datetime64,
):
    """Use the RAINLINK algorithm to classify rain in cml data

    Args:
        cmls (pd.DataFrame): links to be classified
        data_col (pymongo.collection.Collection): Time series cml data
        start_time (np.datetime64): start time for classification
        end_time (np.datetime64): end time for classification
    """

    # Convert np.datetime64 to Python datetime
    start_time_dt = pd.to_datetime(start_time).to_pydatetime()
    end_time_dt = pd.to_datetime(end_time).to_pydatetime()

    # Assume 15 min time steps for now
    time_step = np.timedelta64(15, "m")

    # Loop over the links
    for index, row in cmls.iloc[0:1].iterrows():

        # Get the neibouring links

        # Get the data for this link
        records = []
        query = {
            "link_id": row["link_id"],
            "end_time": {"$gte": start_time_dt, "$lte": end_time_dt},
        }
        for doc in data_col.find(filter=query):
            record = {
                "time": pd.to_datetime(doc["end_time"]),
                "pmax": doc["pmax"]["value"],
                "pmin": doc["pmin"]["value"],
            }
            records.append(record)

        data_df = pd.DataFrame(records)
        data_df.set_index("time", inplace=True)

        # get the neibouring stations


def main():
    """Perform a rain/no-rain classification on link data"""
    parser = argparse.ArgumentParser(
        description="Clissify rain/no_rain in CML data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-s", "--start", type=valid_date, help="Start date yyyy-mm-dd")
    parser.add_argument("-e", "--end", type=valid_date, help="End date yyyy-mm-dd")
    args = parser.parse_args()

    # print out some info
    print(f"Start date = {args.start}\nend date = {args.end}")

    # set up the database
    usr = os.getenv("MONGO_USR")
    pwd = os.getenv("MONGO_PWD")
    if usr is None:
        print("Valid MongoDB user not found", file=sys.stderr)
        sys.exit(1)
    if pwd is None:
        print("Valid MongoDB user password not found", file=sys.stderr)
        sys.exit(1)

    uri_str = "mongodb://localhost:27017"
    # uri_str = f"mongodb+srv://{usr}:{pwd}@wrnz.kej834t.mongodb.net/?retryWrites=true&w=majority"

    myclient = pymongo.MongoClient(uri_str)
    db = myclient["cml"]
    cml_col = db["links"]
    data_col = db["data"]

    # get the list of cmls in the links dictionary in the area that we are working with
    longitude = 4.0
    latitude = 52.0
    max_range = 250000
    cmls = get_cmls(cml_col, longitude, latitude, max_range)
    print(cmls.head())

    # classify rain/no_rain
    # cl_rain(cmls, cml_col, data_col, args.start, args.end)


if __name__ == "__main__":
    main()
