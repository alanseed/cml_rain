"""
    Classify rain / no-rain
    Based on the RAINLINK algorithm
    Assumes that the attenuation has been calculated

"""
import logging
from datetime import datetime
import argparse
import os
import numpy as np
import pymongo
import pymongo.collection
import pandas as pd
from db_utils import get_cmls
import sys

sys.path.append("../scripts")


logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)


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


def is_raining(
    link_id: int,
    neighbours: list,
    time: datetime,
    data_col: pymongo.collection.Collection
) -> bool:
    """
    Use a RAINLINK adjacent algorithm to classify a link as having rain.
    Assumes that attenuation is defined as p_ref - p_min

    Args:
        link_id (int): ID of the target link
        neighbours (list): List of neighbouring links (including the target)
        time (datetime): Time stamp for the observation
        data_col (pymongo.collection.Collection): MongoDB collection with time series data
        min_attenuation (float): Attenuation threshold to classify rain (default: 2 dB/km)

    Returns:
        bool: True if raining, otherwise False.
    """

    min_s_atten = 0.7
    min_atten = 1.4
    # MongoDB query to get neighbours and their attenuation
    query = {
        "link_id": {"$in": neighbours},
        "time.end_time": time,
        "atten.s_atten": {"$ne": float('NaN'), "$type": "double"}
    }

    records = []
    for doc in data_col.find(filter=query, projection={"link_id": 1, "atten": 1, "_id": 0}):
        record = {
            "link_id": int(doc["link_id"]),
            "atten": float(doc["atten"]["atten"]),
            "s_atten": float(doc["atten"]["s_atten"])
        }
        records.append(record)

    if not records:
        return False

    atten_df = pd.DataFrame(records)
    atten_df = atten_df.dropna()
    median_atten = atten_df["atten"].median()
    median_s_atten = atten_df["s_atten"].median()

    if median_atten >= min_atten and median_s_atten >= min_s_atten:
        return True
    else:
        return False


def classify_rain(
    ref_time: datetime,
    cmls: pd.DataFrame,
    cml_col: pymongo.collection.Collection,
    data_col: pymongo.collection.Collection

):
    """
    Use a RAINLINK adjacent algorithm to classify a link with rain based on a neighbourhood search
    The neighbourhood search is set at 10 km
    Updates the has_rain flag in the atten document for each observation

    Args:
        cmls (pd.DataFrame): metadata for links to be processed
        cml_col: (pymongo.collection.Collection): CML metadata
        data_col: (pymongo.collection.Collection): Time series CML data
        ref_time (datetime): Time for processing
    """

    links = cmls["link_id"].values.astype(int).tolist()
    query = {"link_id": {"$in": links}, "time.end_time": ref_time}
    projection = {"link_id": 1, "_id": 0}
    number_links = data_col.count_documents(filter=query)

    # no links found so return
    if number_links == 0:
        return

    max_updates = 1000
    updates = []
    number_rain = 0
    for doc in data_col.find(filter=query, projection=projection):
        link_id = doc.get("link_id")
        if link_id is not None:
            link_id = int(link_id)

            # Get the list of nearest neighbour cmls, including the target cml
            neighbours = []
            max_range = 10000
            mid_lon = float(cmls.loc[cmls["link_id"] == link_id, "mid_lon"].iloc[0])
            mid_lat = float(cmls.loc[cmls["link_id"] == link_id, "mid_lat"].iloc[0])
            n_query = {
                "properties.midpoint": {
                    "$nearSphere": {
                        "$geometry": {"type": "Point", "coordinates": [mid_lon, mid_lat]},
                        "$maxDistance": max_range,
                    }
                }
            }
            n_projection = {"properties.link_id": 1, "_id": 0}

            for n_doc in cml_col.find(filter=n_query, projection=n_projection):
                if n_doc:
                    neighbours.append(int(n_doc["properties"]["link_id"]))

            has_rain = is_raining(link_id, neighbours, ref_time, data_col)

            # assume that the default value for has_rain in the timeseries data is False
            if has_rain:
                number_rain += 1
                atten_doc = {"atten.has_rain": has_rain}

                # Prepare bulk update
                updates.append(pymongo.UpdateOne(
                    {"link_id": link_id, "time.end_time": ref_time},
                    {"$set": atten_doc},
                    upsert=True
                ))
                if len(updates) > max_updates:
                    data_col.bulk_write(updates)
                    updates = []

    # Perform bulk write operation if there are updates
    if updates:
        data_col.bulk_write(updates)

    logging.info(f"Classified rain at {number_rain} links at {ref_time}")
    return


def main():
    """Calculate attenuation and perform a rain/no-rain classification on link data"""
    parser = argparse.ArgumentParser(
        description="Calculate attenuation and classify rain/no_rain in CML data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-s", "--start", type=valid_date,
                        help="Start date yyyy-mm-dd")
    parser.add_argument("-e", "--end", type=valid_date,
                        help="End date yyyy-mm-dd")
    args = parser.parse_args()

    # print out some info
    print(f"Start date = {args.start}\nend date = {args.end}")

    # set up the database

    # usr = os.getenv("MONGO_USR")
    # pwd = os.getenv("MONGO_PWD")
    # if usr is None:
    #     print("Valid MongoDB user not found", file=sys.stderr)
    #     sys.exit(1)
    # if pwd is None:
    #     print("Valid MongoDB user password not found", file=sys.stderr)
    #     sys.exit(1)
    # uri_str = f"mongodb+srv://{usr}:{pwd}@wrnz.kej834t.mongodb.net/?retryWrites=true&w=majority"

    uri_str = "mongodb://localhost:27017"

    myclient = pymongo.MongoClient(uri_str)
    db = myclient["cml"]
    cml_col = db["cml_metadata"]
    data_col = db["cml_data"]

    # get a list of the cmls in the area that we are working with
    longitude = 4.0
    latitude = 52.0
    max_range = 250000
    cmls = get_cmls(cml_col, longitude, latitude, max_range)

    start_time = args.start
    end_time = args.end
    logging.info(f"Start date = {start_time}")
    logging.info(f"End date = {end_time}")

    start_time_dt = pd.to_datetime(start_time).to_pydatetime()
    end_time_dt = pd.to_datetime(end_time).to_pydatetime()
    times = pd.date_range(start=start_time_dt, end=end_time_dt, freq="15min")
    for ref_time in times:
        classify_rain(ref_time, cmls, cml_col, data_col)


if __name__ == "__main__":
    main()
