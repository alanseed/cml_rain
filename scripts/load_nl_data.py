import sys

sys.path.append("../scripts")

from pathlib import Path
import pandas as pd
from pymongo import MongoClient
import numpy as np
import pymongo
import math
from db_utils import is_valid_power 

def write_data_records(data_df, data_col):
    records = []
    batch_size = 10000       
    for index, row in data_df.iterrows():
        p_min = row["Pmin"]
        if not is_valid_power(p_min):
            p_min = float("nan")

        p_max = row["Pmax"]
        if not is_valid_power(p_max):
            p_max = float("nan")

        # Only append records where both p_min and p_max are valid
        if not (math.isnan(p_max) or math.isnan(p_min)):
            record = {
                "link_id": row["ID"],
                "time":{ 
                    "start_time": row["DateTime"] - time_step,
                    "end_time": row["DateTime"]
                },
                "power":{
                    "p_min": p_min,  # Use computed p_min
                    "p_max": p_max,  # Use computed p_max
                },
                "atten":{
                    "p_ref": float("NaN"),
                    "has_rain": False,
                    "atten": float("NaN"),
                    "s_atten": float("NaN")
                }
            }
            records.append(record) 

            # Insert in batches of batch_size
            if len(records) >= batch_size:
                data_col.insert_many(records)
                records = []  # Clear after batch insert

    # Insert any remaining records
    if records:
        data_col.insert_many(records)

    return


# MongoDB connection (Local)
client = MongoClient("mongodb://localhost:27017/")
db = client["cml"]

# Drop existing collections (if needed)
link_col = db["cml_metadata"]
link_col.drop()

# Assume 15 min time steps 
time_step = np.timedelta64(15,'m')

# Read the files into dataframes
file_names = ["CMLs_20110609_20110911_21days.dat", "CMLs_20120530_20120901.dat"]
file_path = Path("/home/alanseed/alan/cml_rain/data", file_names[0])
data_df = pd.read_csv(file_path, sep=" ", header=0)
data_df["DateTime"] = pd.to_datetime(data_df["DateTime"], format="%Y%m%d%H%M")

# Make the list of unique station ids and their metadata
data_df_unique = data_df.drop_duplicates(subset=["ID"]).copy()

# Vectorized midpoint and path length calculations
data_df_unique.loc[:, "midpoint_lon"] = np.round(
    (data_df_unique["XStart"] + data_df_unique["XEnd"]) / 2, 4
)
data_df_unique.loc[:, "midpoint_lat"] = np.round(
    (data_df_unique["YStart"] + data_df_unique["YEnd"]) / 2, 4
)
data_df_unique.loc[:, "path_length"] = (data_df_unique["PathLength"] * 1000).astype(int)

# Build the list of geoJSON features
links = data_df_unique.apply(
    lambda row: {
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": [
                [row["XStart"], row["YStart"]],
                [row["XEnd"], row["YEnd"]],
            ],
        },
        "properties": {
            "link_id": row["ID"],
            "frequency": {"value": row["Frequency"], "units": "GHz"},
            "midpoint": {
                "type": "Point",
                "coordinates": [row["midpoint_lon"], row["midpoint_lat"]],
            },
            "length": {"value": row["path_length"], "units": "m"},
        },
    },
    axis=1,
).tolist()

# Insert the links into MongoDB
link_col.insert_many(links) 
print(f"Found {len(links)} stations")

# Write out the time series data
data_col = db["cml_data"]
data_col.drop()
write_data_records(data_df, data_col)

# now process the second file
file_path = Path("/home/alanseed/alan/cml_rain/data", file_names[1])
print(f"Reading {file_path}")
data_df = pd.read_csv(file_path, sep=" ", header=0)
data_df["DateTime"] = pd.to_datetime(data_df["DateTime"], format="%Y%m%d%H%M")
write_data_records(data_df, data_col)

# Set up the indexes
data_index_id = pymongo.IndexModel(
    [("link_id", pymongo.ASCENDING)], name="link_id")
data_index_id_time = pymongo.IndexModel(
    [("link_id", pymongo.ASCENDING), ("time.end_time", pymongo.ASCENDING)], name="link_id_time"
)
data_col.create_indexes([data_index_id, data_index_id_time])

link_index_id = pymongo.IndexModel(
    [("properties.link_id", pymongo.ASCENDING)], name="link_id", unique=True
)
link_index_midpoint = pymongo.IndexModel(
    [("properties.midpoint", pymongo.GEOSPHERE)], name="midpoint_location"
)
link_col.create_indexes([link_index_midpoint, link_index_id])
