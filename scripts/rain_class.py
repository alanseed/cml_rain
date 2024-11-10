"""
    Classify rain / no-rain 
    Based on the RAINLINK algorithm 
    Assumes that the attenuation has been calculated 

"""
import sys

sys.path.append("../scripts")
from db_utils import get_cmls, is_valid_power

import concurrent.futures
import pandas as pd
import pymongo.collection
import pymongo
import numpy as np
import os
import argparse
from datetime import datetime
import math


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
    data_col: pymongo.collection.Collection,
    min_attenuation: float = 0.7  
) -> bool:
    """
    Use a RAINLINK adjacent algorithm to classify a link as having rain.

    A link is deemed to have rain if more than half of its neighbours 
    have attenuation that is above a specified threshold.
    Attenuation is defined as p_ref - p_min. 

    Args:
        link_id (int): ID of the target link
        neighbours (list): List of neighbouring links (including the target)
        time (datetime): Time stamp for the observation
        data_col (pymongo.collection.Collection): MongoDB collection with time series data
        min_attenuation (float): Attenuation threshold to classify rain (default: 2 dB/km)

    Returns:
        bool: True if raining, otherwise False.
    """
    
    # MongoDB query to get neighbours and their attenuation
    query = {
        "link_id": {"$in": neighbours},
        "time.end_time": time,
        "atten.s_atten": {"$ne": float('NaN'), "$type": "double"}
    }

    # Fetch all records matching the query and convert to a DataFrame
    records = list(
        data_col.find(filter=query, projection={"link_id": 1, "atten.s_atten": 1, "_id": 0})
    )

    if not records:
        return False

    # Create DataFrame directly from query results
    atten_df = pd.DataFrame(records)

    # Check if the target link has a valid observation
    if link_id not in atten_df["link_id"].values:
        return False

    # Extract 's_atten' values from the 'atten' column
    atten_df["s_atten"] = atten_df["atten"].apply(lambda x: x.get("s_atten", float('nan'))) 

    # Calculate how many links (including the target) have attenuation above the threshold
    number_with_rain = atten_df[atten_df["s_atten"] > min_attenuation].shape[0]
    number_links = len(records)
    min_raining_links = 0.5 * number_links

    # Classification logic
    if (number_with_rain == 1 and number_links <= 2) or number_with_rain >= min_raining_links:
        return True
    
    return False

def classify_rain(
    cml: dict,
    cml_col: pymongo.collection.Collection,
    data_col: pymongo.collection.Collection,
    start_time: np.datetime64,
    end_time: np.datetime64,
):
    """
    Use a RAINLINK adjacent algorithm to classify a link with rain based on a neighbourhood search

    The neighbourhood is set at 15 km

    Args:
        cml (dict): link to be processed
        cml_col: (pymongo.collection.Collection): CML metadata
        data_col: (pymongo.collection.Collection): Time series CML data
        start_time (np.datetime64): start time for processing
        end_time (np.datetime64): end time for processing
    """

    link_id = int(cml["link_id"])

    # Get the list of nearest neighbour cmls, including the target cml
    neighbours = []
    max_range = 15000
    mid_lon = cml["mid_lon"]
    min_lat = cml["mid_lat"]
    query = {
        "properties.midpoint": {
            "$nearSphere": {
                "$geometry": {"type": "Point", "coordinates": [mid_lon, min_lat]},
                "$maxDistance": max_range,
            }
        }
    }
    for doc in cml_col.find(filter=query, projection={"properties.link_id": 1}):
        neighbours.append(doc["properties"]["link_id"])

    # Set up the times to be processed
    start_time_dt = pd.to_datetime(start_time).to_pydatetime()
    end_time_dt = pd.to_datetime(end_time).to_pydatetime()
    time_range = pd.date_range(
        start=start_time_dt, end=end_time_dt, freq="15min")

    updates = []  # List to store updates for bulk write
    max_updates = 1000 

    # loop over the time steps
    for time in time_range:

        # check that there is a record at this link for this time
        has_record = data_col.count_documents(
            filter={"link_id": link_id, "time.end_time": time}
        )
        if has_record == 1:
            has_rain = is_raining(link_id, neighbours, time, data_col)
            if has_rain:
                rain_doc = {"atten.has_rain": has_rain}

                # Prepare bulk update
                updates.append(pymongo.UpdateOne(
                    {"link_id": link_id, "time.end_time": time},
                    {"$set": rain_doc},
                    upsert=True
                ))
                if len(updates) > max_updates:
                    data_col.bulk_write(updates)
                    updates = []

    # Perform bulk write operation if there are updates
    if updates:
        data_col.bulk_write(updates)
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

    # get the list of cmls in the links dictionary in the area that we are working with
    longitude = 4.0
    latitude = 52.0
    max_range = 250000
    cmls = get_cmls(cml_col, longitude, latitude, max_range)
    links = cmls.to_dict(orient="records")

    # process the links
    start_time = args.start
    end_time = args.end

    num_workers = 32  # Number of cores
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(classify_rain, link, cml_col,
                            data_col, start_time, end_time)
            for link in links
        ]

    # Ensure all threads complete by checking the results
    for future in futures:
        future.result()

    # for link in links:
    #     print(f"Processing link {link["link_id"]}")
    #     classify_rain(link, cml_col, data_col, start_time, end_time)


if __name__ == "__main__":
    main()
