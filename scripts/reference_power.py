"""
Generate the reference power for each day 

Reference power is defined as the maximum valid Pmin for the last 24 hours

"""

import sys

sys.path.append("../scripts")
import concurrent.futures

from datetime import datetime, timedelta
import argparse
import os
import numpy as np
import pymongo
import pymongo.collection
import pandas as pd
import time 

from db_utils import get_cmls, calc_p_ref

import logging
logging.basicConfig(level=logging.INFO)

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


def calculate_ref_power(ref_time:datetime, links:int, data_col:pymongo.collection.Collection):
    """Calculate reference power for a set of links at ref_time

    Args:
        ref_time (datetime): Time
        links ([int]): List of links to be processed
        data_col (pymongo.collection.Collection): data collection 
    """    

    # get the links with data at this time step 
    query = {"link_id":{"$in":links}, "time.end_time":ref_time}
    projection = {"link_id":1, "_id":0}
    number_links = data_col.count_documents(filter=query)

    # no links found so return 
    if number_links == 0:
        return 
    
    max_updates = 1000 
    updates = [] 
    for doc in data_col.find(filter=query, projection=projection): 
        link_id = doc["link_id"] 

        # Calculate the reference power
        p_ref = calc_p_ref(link_id, data_col, ref_time)
        p_ref_doc = {"atten.p_ref": p_ref}

        # Prepare bulk update
        updates.append(pymongo.UpdateOne(
            {"link_id": link_id, "time.end_time": ref_time},
            {"$set": p_ref_doc},
            upsert=True
        ))
        if len(updates) > max_updates:
            data_col.bulk_write(updates)
            updates = []

    # Perform any remaining bulk write operations
    if updates:
        data_col.bulk_write(updates)

    logging.info(f"Updated {number_links} links at {ref_time}")

def main():
    """Calculate the maximum valid Pmin over a 24 h period"""
    parser = argparse.ArgumentParser(
        description="Calculate the maximum valid Pmin over 24 h",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-s", "--start", type=valid_date, help="Start date yyyy-mm-dd")
    parser.add_argument("-e", "--end", type=valid_date, help="End date yyyy-mm-dd")
    args = parser.parse_args()

    # print out some info
    start_time = args.start
    end_time = args.end
    logging.info(f"Start date = {start_time}") 
    logging.info(f"End date = {end_time}")

    # set up a local database
    uri_str = "mongodb://localhost:27017"

    # setup a MongoDB Atlas database
    # usr = os.getenv("MONGO_USR")
    # pwd = os.getenv("MONGO_PWD")
    # if usr is None:
    #     print("Valid MongoDB user not found", file=sys.stderr)
    #     sys.exit(1)
    # if pwd is None:
    #     print("Valid MongoDB user password not found", file=sys.stderr)
    #     sys.exit(1)
    # uri_str = f"mongodb+srv://{usr}:{pwd}@wrnz.kej834t.mongodb.net/?retryWrites=true&w=majority"

    myclient = pymongo.MongoClient(uri_str)
    db = myclient["cml"]
    cml_col = db["cml_metadata"]
    data_col = db["cml_data"]

    # get the list of cmls in the links dictionary in the area that we are working with
    longitude = 4.0
    latitude = 52.0
    max_range = 250000
    cmls = get_cmls(cml_col, longitude, latitude, max_range)
    links = cmls["link_id"].values.tolist() 

    # make the list of 15 min times to be processed 
    start_time_dt = pd.to_datetime(start_time).to_pydatetime()
    end_time_dt = pd.to_datetime(end_time).to_pydatetime()
    times = pd.date_range(start=start_time_dt, end=end_time_dt, freq="15min")
    for ref_time in times:
        calculate_ref_power(ref_time, links, data_col)

if __name__ == "__main__":
    main()
