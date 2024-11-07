# Classify the raining periods using the RAINLINK algorithm as a guide
from datetime import datetime
import argparse
import os
import sys
import numpy as np
import pymongo
import pymongo.collection
import pandas as pd
import concurrent.futures


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
    """
    Return the CMLs that are within a radius of a location

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
    projection = {
        "properties.link_id": 1,
        "properties.frequency": 1,
        "properties.length": 1,
        "properties.midpoint.coordinates": 1,
    }
    records = [
        {
            "link_id": doc["properties"]["link_id"],
            "frequency": float(doc["properties"]["frequency"]["value"]),
            "length": float(doc["properties"]["length"]["value"]),
            "mid_lon": float(doc["properties"]["midpoint"]["coordinates"][0]),
            "mid_lat": float(doc["properties"]["midpoint"]["coordinates"][1]),
        }
        for doc in cml_col.find(filter=query, projection=projection)
    ]

    cml_df = pd.DataFrame(records)
    return cml_df


def is_valid_power(power: float) -> bool:
    """
    Check that the link power is within a valid range

    Args:
        power (float): Link power to be checked

    Returns:
        bool: True if within the range 
    """
    # Valid range for pmax or pmin based on the PDF of the Netherlands link data
    max_valid_power = -20
    min_valid_power = -70
    if (power >= min_valid_power) & (power <= max_valid_power):
        return True
    else:
        return False


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
    max_rain_power = -55
    has_rain = False

    # get the valid observations for the links
    query = {"link_id": {"$in": neighbours}, "end_time": time}
    records = []

    for doc in data_col.find(
        filter=query, projection={"link_id": 1, "pmax.value": 1, "_id": 0}
    ):
        power = float(doc["pmax"]["value"])
        if is_valid_power(power):
            record = {"link_id": doc["link_id"], "power": power}
            records.append(record)

    if records:
        power_df = pd.DataFrame(records)

        # check that the target link has a valid observation
        if link_id in power_df["link_id"].values:

            number_neighbours = len(record)

            # get the number of links that have power less than max_rain_power
            # includes the target link
            number_with_rain = len(power_df[power_df["power"] < max_rain_power])

            # more than half of the neighbours need to be below the rain threshold
            min_raining_neighbours = 0.5 * number_neighbours

            # use a simple threshold if the target link has rain and here are no neighbours
            if (number_with_rain == 1) & (number_neighbours <= 2):
                has_rain = True

            # else check if we have enough neighbouring links with rain
            # the target link may, or, may not, have rain
            elif number_with_rain >= min_raining_neighbours:
                has_rain = True
    return has_rain


def cml_rain(
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
    time_range = pd.date_range(start=start_time_dt, end=end_time_dt, freq="15min")

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
                {"$push": {"rain": rain_doc}},
                upsert=True,
            )


def main():
    """Perform a rain/no-rain classification on link data"""
    parser = argparse.ArgumentParser(
        description="Classify rain/no_rain in CML data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-s", "--start", type=valid_date, help="Start date yyyy-mm-dd")
    parser.add_argument("-e", "--end", type=valid_date, help="End date yyyy-mm-dd")
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
    data_col = db["data"]

    # get the list of cmls in the links dictionary in the area that we are working with
    longitude = 4.0
    latitude = 52.0
    max_range = 250000
    cmls = get_cmls(cml_col, longitude, latitude, max_range)
    links = cmls.to_dict(index=False)

    # process the links
    num_workers = 32  # Number of cores
    start_time = args.start
    end_time = args.end
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(cml_rain, link, cml_col, data_col, start_time, end_time)
            for link in links
        ]

    # Ensure all threads complete by checking the results
    for future in futures:
        future.result()

if __name__ == "__main__":
    main()
