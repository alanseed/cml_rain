"""
    calculate the attenuation and classify rain / no-rain 
    Based on the RAINLINK algorithm 

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

def calc_attenuation(doc: dict) -> float:
    """
    Calculate the attenuation.

    Args:
        doc (dict): document from the cml[data] database.

    Returns:
        float: attenuation in dBm or NaN.
    """    
    p_min = doc.get("power", {}).get("p_min")
    p_ref = doc.get("atten", {}).get("p_ref")
    
    if p_min is not None and p_ref is not None:
        p_min = float(p_min)
        p_ref = float(p_ref)

        if is_valid_power(p_min) and is_valid_power(p_ref):
            return p_ref - p_min

    return float("NaN")

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

def link_attenuation(
    cml: dict,
    data_col: pymongo.collection.Collection,
    start_time: np.datetime64,
    end_time: np.datetime64,
):
    """
    Calculate the attenuation for a link over a period of time.

    Args:
        cml (dict): link to be processed.
        data_col (pymongo.collection.Collection): Time series CML data.
        start_time (np.datetime64): start time for processing.
        end_time (np.datetime64): end time for processing.
    """
    # unfortunately the link ID is still int32
    link_id = int(cml["link_id"])
    length = float(cml["length"]) / 1000.0  # length in km

    # Convert times once at the start
    start_time_dt = pd.to_datetime(start_time).to_pydatetime()
    end_time_dt = pd.to_datetime(end_time).to_pydatetime()
    
    # assumes 15 min time steps 
    time_range = pd.date_range(start=start_time_dt, end=end_time_dt, freq="15min")

    updates = []  # List to store updates for bulk write
    max_updates = 1000 

    # Loop over the time steps
    for time in time_range:
        doc = data_col.find_one({"link_id": link_id, "time.end_time": time},
                                projection={"power": 1, "atten": 1})

        if doc:
            atten = calc_attenuation(doc)
            if not math.isnan(atten):
                s_atten = atten / length  # specific attenuation
                atten_doc = {"atten.atten": atten, "atten.s_atten": s_atten}

                # Prepare bulk update
                updates.append(pymongo.UpdateOne(
                    {"link_id": link_id, "time.end_time": time},
                    {"$set": atten_doc},
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

    # get a list of the cmls in the area that we are working with
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
            executor.submit(link_attenuation, link,
                            data_col, start_time, end_time)
            for link in links
        ]

    # Ensure all threads complete by checking the results
    for future in futures:
        future.result()

    # for link in links:
    #     print(f"Processing link {link["link_id"]}")
    #     link_attenuation(link, data_col, start_time, end_time)

if __name__ == "__main__":
    main()
