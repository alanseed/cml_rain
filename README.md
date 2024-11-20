# CML_RAIN  
This is a proof of concept project that uses Python, MongoDB, and SNMP to configure and collect signal attenuation data from a network of Commercial Microwave Links.   

The algorithms are based on:  

Overeem, A., Leijnse, H., and Uijlenhoet, R.: Retrieval algorithm for rainfall mapping from microwave links in a cellular communication network, Atmos. Meas. Tech., 9, 2425–2444, https://doi.org/10.5194/amt-9-2425-2016, 2016.  


The objective of this POC is to test the performance when using a MongoDB database to manage the data store. A first attempt at a database design is presented and some of the more data intensive algorithms are implemented. The resulting products should not be taken as quantitative rainfall estimations, rather a test to find out if the general design is feasible.  

## Load test data  

`scripts/load_nl_data.py` reads the open source data from Wageningen University [4TU.ResearchData](https://data.4tu.nl/articles/dataset/Commercial_microwave_link_data_for_rainfall_monitoring/12688253) 
and ceates the MongoDB database of ~3000 links with ~30 million records.  

## CML Metadata  

Each link is saved as the "cml_metadata" collection in the "cml" data base.  
The collection is a set geoJSON documents, each with a unique link_id string.  The mid-point of the link is used as the location index for spatial searches from MongoDB.  

 
```json
{
    "collection_name": "cml_data",
    "fields":{   
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": [
            ["XStart", "YStart"],  // Starting coordinates (lon, lat)
            ["XEnd", "YEnd"]       // Ending coordinates (lon, lat)
            ]
        },
        "properties": {
            "link_id": "String",  // Link ID (unique identifier)
            "frequency": {
            "value": "Number",  // Frequency value
            "units": "GHz"      // Frequency units
            },
            "midpoint": {
            "type": "Point",  // Midpoint type
            "coordinates": ["midpoint_lon", "midpoint_lat"]  // Midpoint coordinates
            },
            "length": {
            "value": "Number",  // Path length value
            "units": "m"        // Path length units (meters)
            }
        },
    }        
    "indexes": [
        { "fields": ["properties.midpoint"], "type": "2dsphere" },
        { "fields": ["link_id"], "type": "single" }
    ]  
}

```  

## CML time series data  

The time series data for minimum and maximum power, are stored in the "cml_data" collection. Each observation is saved as a document. The link_id and end_time are used as a compound index for searches on this collection. 

```json
{
  "collection_name": "cml_data",
  "fields": {
    "link_id": "String",  // Link ID (unique identifier)
    "time": {
      "start_time": "ISODate",  // Start of the sampling period UTC
      "end_time": "ISODate"  // End of the sampling period UTC
    },
    "power": {
      "p_min": "Float",  // Minimum power in the sampling period in dBm 
      "p_max": "Float"  // Maximum power in the sampling period in dBm
    },
    "atten": {
      "p_ref": "Float",  // Maximum power in the last n hours in dBm
      "has_rain": "Boolean",  // Rain / no rain classification
      "atten": "Float",  // Attenuation (pref-pmin) in dBm
      "s_atten": "Float"  // Specific attenuation in dBm / km
    },
    "rain": "Float"  // Rain rate in mm / h
  },
  "indexes": [
    { "fields": ["link_id", "time.end_time"], "type": "compound" },
    { "fields": ["link_id"], "type": "single" }
  ]
}
```  

# Reference power  

Following Overeem et al (2016) the attenuation is calculated as the difference between a reference power and the measured p_min over the interval. The reference power is calculated using `scripts/reference_power.py` for given start and end ISODates (yyyy-mm-dd). The script is configured to search the cml_metadata collection for the links that are within 250 km of a central location:  

```python
    # get the list of cmls in the links dictionary in the area that we are working with
    longitude = 4.0
    latitude = 52.0
    max_range = 250000
    cmls = get_cmls(cml_col, longitude, latitude, max_range)

```  

This algorithm generates a list of links that are in the search area. For each 15-min time step the algorith searches the database for any link in the list that has data for that time step and calculates the maximum p_min over the preceeding 24 h.  

It takes just under 2 minutes to process all ~3000 links for each 15-minute timestep in a day.

## Usage  

scripts/reference_power.py --start yyyy-mm-dd --end yyyy-mm-dd  
where yyyy-mm-dd represents the desired start and end dates  

## Output  

The script updates the "atten.p_ref" fields  

# Attenuation  

`scripts/attenuation.py` assumes that the reference power has been calculated and uses this to calculate the attenuation, based on p_min.  

The complexity here is the link length needs to be known for the calculation of specific attenuation and this comes from the metadata collection, so it is an example of combining information from both the cml_metadata and cml_data collections.  

## Usage  

scripts/attenuation.py --start yyyy-mm-dd --end yyyy-mm-dd  
where yyyy-mm-dd represents the desired start and end dates  

## Output  

The script updates the "atten.atten" and "atten.s_atten" fields in the database  

# Rain / no rain classification  

`scripts/rain_class.py` implements the RAINLINK algorithm to classify a link with rain.  

This algorithm invokes a spatial search for the links within a range of the link and with valid attenuation fields and classifies rain if the median attenuation and specific attenuation in the neighbourhood exceeds certain limits.  

The neighbours within 10 km of a link with valid data for the reference time are identfied and the attenuation and specific attenuation for each of the neighbouring links are etracted from the database.

It takes 5-10 seconds to perform this classification on network of 3000 links.  

## Usage  

scripts/rain_class.py --start yyyy-mm-dd --end yyyy-mm-dd  
where yyyy-mm-dd represents the desired start and end dates  

## Output  

The script updates the "atten.has_rain" field to True if rain has been detected  

# Attenuation to rain  

`scripts/rain.py` implements the ITUR-R P.838-3 recommendation for a specific attenuation model to estimate the link mean rain rate based on the frequency of the link and the specific attenuation estimate.

## Usage  

scripts/rain.py --start yyyy-mm-dd --end yyyy-mm-dd  
where yyyy-mm-dd represents the desired start and end dates  

## Output  

The script inserts or updates the "rain" sub-document.  

The default is for the rain sub-document to be missing after the initial record is inserted, and this is added once the rain is estimated.  

# cml_interpolate  

## Dependencies  
On fedora:  
Basic c++ tools  
cmake  
g++  
gcc  
pkg-config  

sudo dnf install gsl gsl-devel  
sudo dnf install blas blas-devel lapack lapack-devel  
sudo dnf install udunits2  
sudo dnf install nlohmann-json-devel  

[mongo-cxx-driver](https://www.mongodb.com/docs/languages/cpp/cpp-driver/current/)  

The netCDF is installed in this order:  
sudo dnf install hdf5 hdf5-devel  
sudo dnf install netcdf netcdf-devel  

