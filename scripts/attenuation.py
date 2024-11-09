"""
    Classify rain / no-rain along a link 
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

def calc_attenuation(doc:dict)->float:
    """
    Calculate the attenuation 

    Args:
        doc (dict): document from the cml[data] database 

    Returns:
        float: attenuation in dBm or NaN
    """    
    p_atten = float("NaN")
    pmin = doc.get("pmin", {}).get("value") 
    if pmin is not None:
        pmin = float(pmin)
        if is_valid_power(pmin):
            max_pmin = doc.get("max_pmin")
            if max_pmin is not None:
                max_pmin = float(max_pmin)
                if is_valid_power(max_pmin):
                    max_pmin = float(max_pmin)
                    p_atten = max_pmin - pmin
    
    return p_atten


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
    link_id: str,
    neighbours: list,
    time: datetime,
    data_col: pymongo.collection.Collection,
) -> bool:
    """
    Use a RAINLINK adjacent algorithm to classify a link as having rain

    A link is deemed to have rain with more than half of the neighbours have
    power that is below the rain threshold

    The rain threshold is set at the 10 percentile value of the power
    which is more or less the climatological probability of rain in 15 minutes

    Args:
        link_id (str): ID of the target link
        neighbours (list): List of neighbours (including the target)
        time (datetime): Time stamp for the observation
        data_col (pymongo.collection.Collection): Mongo DB collection with the time series data

    Returns:
        bool: True if raining
    """
    min_attenuation =  2
    has_rain = False

    # get the valid observations for the links
    query = {"link_id": {"$in": neighbours}, "end_time": time}
    records = []

    for doc in data_col.find(
        filter=query, projection={"link_id": 1, "pmin.value": 1, "max_pmin":1,"_id": 0}
    ):
        p_atten = calc_attenuation(doc)
        if not math.isnan(p_atten):
            record = {"link_id": doc["link_id"], "p_atten": p_atten}
            records.append(record)
        
    number_links = len(record)

    if number_links > 0:
        atten_df = pd.DataFrame(records)

        # check that the target link has a valid observation
        if link_id in atten_df["link_id"].values:

            # get the number of links that have enough attenuation for rain
            # includes the target link
            number_with_rain = len(
                atten_df[atten_df["p_atten"] > min_attenuation])

            # more than half of the neighbours need to be below the rain threshold
            min_raining_links = 0.5 * number_links

            # use a simple threshold if the target link has rain and here are no neighbours
            if (number_with_rain == 1) and (number_links <= 2):
                has_rain = True

            # else check if we have enough neighbouring links with rain
            # the target link may, or, may not, have rain
            elif number_with_rain >= min_raining_links:
                has_rain = True
    return has_rain


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

    link_id = cml["link_id"]

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

    # loop over the time steps
    for time in time_range:

        # check that there is a record at this link for this time
        has_record = data_col.count_documents(
            filter={"link_id": link_id, "end_time": time}
        )
        if has_record == 1:
            # check if there is rain along this link
            rain = is_raining(link_id, neighbours, time, data_col)
            rain_doc = {"rain": rain}

            # append rain_doc to the record
            data_col.update_one(
                {"link_id": link_id, "end_time": time},
                {"$push": rain_doc},
                upsert=True,
            )


def main():
    """Perform a rain/no-rain classification on link data"""
    parser = argparse.ArgumentParser(
        description="Classify rain/no_rain in CML data",
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
    cml_col = db["links"]
    data_col = db["test_data"]

    # get the list of cmls in the links dictionary in the area that we are working with
    longitude = 4.0
    latitude = 52.0
    max_range = 250000
    cmls = get_cmls(cml_col, longitude, latitude, max_range)
    links = cmls.to_dict(orient="records")

    # process the links
    num_workers = 32  # Number of cores
    start_time = args.start
    end_time = args.end
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(classify_rain, link, cml_col,
                            data_col, start_time, end_time)
            for link in links
        ]

    # Ensure all threads complete by checking the results
    for future in futures:
        future.result()


if __name__ == "__main__":
    main()
