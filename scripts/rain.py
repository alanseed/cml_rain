"""
    Use attenuation to estimate the link mean rain rate

    Raises:
        argparse.ArgumentTypeError: _description_

    Returns:

"""

import logging
from datetime import datetime
import argparse
import os
import itur.models
import itur.models.itu838
import numpy as np
import pymongo
import pymongo.collection
import pandas as pd
import math
import itur
import astropy.units as u
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


def estimate_rain(
        ref_time: datetime,
        cmls: pd.DataFrame,
        data_col: pymongo.collection.Collection):
    """
    Use specific attenuation to estimate rain rate

    Args:
        ref_time (datetime): _description_
        cmls (pd.DataFrame): _description_
        cml_col (pymongo.collection.Collection): _description_
        data_col (pymongo.collection.Collection): _description_
    """
    links = cmls["link_id"].values.astype(int).tolist()
    query = {"link_id": {"$in": links}, "time.end_time": ref_time}
    projection = {"link_id": 1, "atten": 1, "_id": 0}
    number_links = data_col.count_documents(filter=query)

    # no links found so return
    if number_links == 0:
        return

    max_updates = 1000
    updates = []
    for doc in data_col.find(filter=query, projection=projection):
        link_id = int(doc["link_id"])

        # estimate the rain rate
        rain_rate = 0.0

        gamma = float(doc["atten"]["s_atten"])
        if gamma > 0:
            freq = float(cmls.loc[cmls["link_id"] == link_id, "frequency"].iloc[0])
            f = freq * u.GHz
            k, alpha = itur.models.itu838.rain_specific_attenuation_coefficients(
                f, 0.0, 0.0)
            rain_rate = np.pow(gamma/k,1/alpha)
            rain_rate = np.round(rain_rate,decimals=2)

        if not math.isnan(rain_rate):
            rain_doc = {"rain": float(rain_rate)}

            # Prepare bulk update
            updates.append(pymongo.UpdateOne(
                {"link_id": link_id, "time.end_time": ref_time},
                {"$set": rain_doc},
                upsert=True
            ))
            if len(updates) > max_updates:
                data_col.bulk_write(updates)
                updates = []

    # Perform bulk write operation if there are updates
    if updates:
        data_col.bulk_write(updates)

    logging.info(
        f"Updated rain rate estimation at {number_links} links at {ref_time}")


def main():
    """Use attenuation to estimate link rain rate"""
    parser = argparse.ArgumentParser(
        description="Use attenuation to estimate link mean rain rate ",
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
        estimate_rain(ref_time, cmls, data_col)


if __name__ == "__main__":
    main()
