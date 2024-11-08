import pandas as pd
from datetime import datetime, timedelta
import pandas as pd
import pymongo
import pymongo.collection

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
    
def calc_max_pmin(
    link_id: int, data_col: pymongo.collection.Collection, time: datetime
) -> float:
    """
    Calculate the maximum valid Pmin in the last 24 hours
    Args:
        link_id (int): _description_
        data_col (pymongo.collection.Collection): _description_
        time (datetime): _description_
    """
    ref_power = float("NaN")
    min_number_records = 25
    start_time = time - timedelta(days=1)
    query = {"link_id": link_id, "end_time": {"$gte":start_time, "$lte":time}}
    projection = {"pmin.value":1, "_id":0}
    number_records = data_col.count_documents(
        filter=query
    )
    if number_records > min_number_records:
        pmin = []
        for doc in data_col.find(filter=query, projection = projection):
            value = float(doc["pmin"]["value"])
            if is_valid_power(value):
                pmin.append(value)
        
        # calculate the max if we have enough valid values
        if pmin:   
            data = np.array(pmin, dtype=float)
            if len(data) > min_number_records:
                ref_power = data.max()
    return ref_power
