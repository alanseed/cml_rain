# CML_RAIN  
This is a proof of concept project that uses Python, MongoDB, and SNMP to configure and collect signal attenuation data from a network of Commercial Microwave Links  

## Load test data  

`load_nl_data.py` reads open source data from Netherlands https://data.4tu.nl/articles/dataset/Commercial_microwave_link_data_for_rainfall_monitoring/12688253 and ceates the MongoDB link dictionary and the timeseries database 

## Link Dictionary  

Each link is saved as the "links" colletion in the "cml" data base. The collection is a set geoJSON documents, each with a unique link_id string.  The mid-point of the link is used as the location index for spatial searches from MongoDB.  

```python

{
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
} 
```  
## Time series data   

The time series data for minimum and maximum power, are stored in the "data" collection. Each observation is saved as a document.  

``` python
    record = {
        "link_id": row["ID"],
        "time": row["DateTime"],
        "pmin": {"value": row["Pmin"], "units": "dBm"},
        "pmax": {"value": row["Pmax"], "units": "dBm"}, 
    }
```  
The exact definition of "time" is not clear at this time. My current working assumption is that it is the min and max over the PAST 15 minutes.  

The (time, link_id) pair is assumed to be unique. 
