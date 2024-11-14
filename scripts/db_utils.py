import pandas as pd
from datetime import datetime, timedelta
import pandas as pd
import pymongo
import pymongo.collection
import numpy as np
import math

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
            "link_id": int(doc["properties"]["link_id"]),
            "frequency": float(doc["properties"]["frequency"]["value"]),
            "length": float(doc["properties"]["length"]["value"]),
            "mid_lon": float(doc["properties"]["midpoint"]["coordinates"][0]),
            "mid_lat": float(doc["properties"]["midpoint"]["coordinates"][1]),
        }
        records.append(record)

    # Convert the list of records to a DataFrame
    cml_df = pd.DataFrame(records)
    cml_df = cml_df.loc[(cml_df["length"] > 500) & (cml_df["length"] < 10000) ]
    cml_df = cml_df.loc[(cml_df["frequency"] > 10.0) & (cml_df["frequency"] < 40.0)]

    return cml_df

def is_valid_power(power: float) -> bool:
    """
    Check that the link power is within a valid range

    Args:
        power (float): Link power to be checked

    Returns:
        bool: True if within the range 
    """

    if math.isnan(power) or (power is None):
        return False 
    
    # Valid range for pmax or pmin based on the PDF of the Netherlands link data
    max_valid_power = -20
    min_valid_power = -70
    if (power >= min_valid_power) & (power <= max_valid_power):
        return True
    else:
        return False
    

def calc_p_ref(link_id: int, data_col: pymongo.collection.Collection, time: datetime) -> float:
    """
    Calculate the P_ref in the last 24 hours.
    P_ref is defined as the median (P_min+P_max)/2 over the previous 24 h.
    Returns NaN if a valid P_ref has not been calculated 

    Args:
        link_id (int): Link ID. as an integer
        data_col (pymongo.collection.Collection): MongoDB collection.
        time (datetime): Time to reference for the last 24 hours.    
    """
    ref_power = float("NaN")
    min_number_records = 25
    start_time = time - timedelta(days=1)

    # Query MongoDB for dry periods without rain
    query = {
        "link_id": link_id,
        "time.end_time": {"$gte": start_time, "$lte": time},
        "atten.has_rain": False
    }
    projection = {"power": 1, "_id": 0}

    # Aggregate directly if records exist
    records = list(data_col.find(filter=query, projection=projection))
    if len(records) > min_number_records:
        pave = []
        for doc in records:
            pmin = doc.get("power", {}).get("p_min")
            pmax = doc.get("power", {}).get("p_max")
            if pmin is not None and pmax is not None:
                try:
                    value = (float(pmin) + float(pmax)) / 2.0
                    if is_valid_power(value):
                        pave.append(value)
                except (ValueError, TypeError):
                    continue

        # Calculate the median if we have enough valid data
        if len(pave) >= min_number_records:
            ref_power = np.median(pave)

    return ref_power

