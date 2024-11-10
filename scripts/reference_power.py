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

from db_utils import get_cmls, calc_max_pmin


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


def calculate_ref_power(
    cml: dict,
    data_col: pymongo.collection.Collection,
    start_time: np.datetime64,
    end_time: np.datetime64,
):
    # unfortunately link_id is int32 in the database
    link_id = int(cml["link_id"])

    start_time_dt = pd.to_datetime(start_time).to_pydatetime()
    end_time_dt = pd.to_datetime(end_time).to_pydatetime()
    time_range = pd.date_range(start=start_time_dt, end=end_time_dt, freq="15min")

    updates = []  # List to store updates for bulk write
    max_updates = 1000 

    # Loop over the time steps
    for time in time_range:

        # Check that there is a record at this link for this time
        has_record = data_col.count_documents(
            filter={"link_id": link_id, "time.end_time": time}
        )
        if has_record == 1:
            # Calculate the reference power
            p_ref = calc_max_pmin(link_id, data_col, time)
            p_ref_doc = {"atten.p_ref": p_ref}

            # Prepare bulk update
            updates.append(pymongo.UpdateOne(
                {"link_id": link_id, "time.end_time": time},
                {"$set": p_ref_doc},
                upsert=True
            ))
            if len(updates) > max_updates:
                data_col.bulk_write(updates)
                updates = []

    # Perform any remaining bulk write operations
    if updates:
        data_col.bulk_write(updates)


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
    print(f"Start date = {start_time}\nEnd date = {end_time}")

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
    links = cmls.to_dict(orient="records")

    # process the links in parallel
    num_workers = 32  # Number of cores
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(calculate_ref_power, link, data_col, start_time, end_time)
            for link in links
        ]

    # Ensure all threads complete by checking the results
    for future in futures:
        future.result()

    # for link in links:
    #     print(f"Processing link {link["link_id"]}")
    #     calculate_ref_power(link, data_col, start_time, end_time)


if __name__ == "__main__":
    main()
