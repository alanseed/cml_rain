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

import logging
logging.basicConfig(format = '%(asctime)s %(message)s',level=logging.INFO) 

def calc_atten(doc: dict) -> float:
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


def calculate_attenuation(ref_time:datetime, cmls:pd.DataFrame, data_col:pymongo.collection.Collection):
    """
    Calculate the attenuation for a set of links at a time
    Assumes that the reference power has been calculated 

    Args:
        ref_time (datetime): _description_
        cml (dict): Dictionary of link metadata in the area of interest 
        data_col (pymongo.collection.Collection): _description_
    """
    links = cmls["link_id"].values.astype(int).tolist() 
    query = {"link_id":{"$in":links}, "time.end_time":ref_time}
    projection = {"link_id":1, "power":1, "atten":1,"_id":0}
    number_links = data_col.count_documents(filter=query)

    # no links found so return 
    if number_links == 0:
        return 
    
    max_updates = 1000 
    updates = [] 
    for doc in data_col.find(filter=query, projection=projection): 
        link_id = doc["link_id"] 
        length = float(cmls.loc[cmls["link_id"] == link_id]["length"]) / 1000.0  # length in km

        atten = calc_atten(doc)
        if not math.isnan(atten) and length > 0:
            s_atten = atten / length  # specific attenuation
            atten_doc = {"atten.atten": atten, "atten.s_atten": s_atten}

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

    logging.info(f"Updated attenuation at {number_links} links at {ref_time}")



def main():
    """Calculate attenuation over a set of links in an area of interest"""
    parser = argparse.ArgumentParser(
        description="Calculate attenuation for a set of links",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-s", "--start", type=valid_date,
                        help="Start date yyyy-mm-dd")
    parser.add_argument("-e", "--end", type=valid_date,
                        help="End date yyyy-mm-dd")
    args = parser.parse_args()


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
        calculate_attenuation(ref_time, cmls, data_col)


if __name__ == "__main__":
    main()
