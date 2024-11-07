from datetime import datetime

import numpy as np
import pymongo
import pymongo.collection

uri_str = "mongodb://localhost:27017"

myclient = pymongo.MongoClient(uri_str)
db = myclient["cml"]
cml_col = db["links"]
data_col = db["data"]
test_data_col = db["test_data"]
test_data_col.drop()

start_time = datetime(year=2011, month=6,day=9) 
end_time = datetime(year=2011, month=7,day=1)

records = []
for doc in data_col.find(
    filter={ "end_time":{"$gte":start_time, "$lte":end_time}},
    projection={"_id":0}):
    records.append(doc)

test_data_col.insert_many(records)

# Set up the indexes
data_index_id = pymongo.IndexModel(
    [("link_id", pymongo.ASCENDING)], name="link_id")
data_index_id_time = pymongo.IndexModel(
    [("link_id", pymongo.ASCENDING), ("end_time", pymongo.ASCENDING)], name="link_id_time"
)
test_data_col.create_indexes([data_index_id, data_index_id_time])
