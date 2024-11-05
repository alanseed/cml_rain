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
            "link_id": doc["properties"]["link_id"],
            "frequency": float(doc["properties"]["frequency"]["value"]),
            "length": float(doc["properties"]["length"]["value"]),
            "mid_lon": float(doc["properties"]["midpoint"]["coordinates"][0]),
            "mid_lat": float(doc["properties"]["midpoint"]["coordinates"][1]),
        }
        records.append(record)

    # Convert the list of records to a DataFrame
    cml_df = pd.DataFrame(records)
    return cml_df