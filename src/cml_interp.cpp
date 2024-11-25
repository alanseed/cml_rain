#include "cml_interp.h"
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/database.hpp>
#include <mongocxx/exception/query_exception.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

#include <cassert>
#include <cmath> // for HUGE_VAL
#include <iomanip> // for std::setprecision()
#include <iostream>
#include <ncType.h>
#include <netcdf>

/// @brief Set up the MongoDB client manager for this class
CmlInterp::CmlInterp() { _client = &MongoClientManager::get_client(); }
/// @brief Set up the map domain
/// @param config JSON configuration
void CmlInterp::set_config(json config)
{
    _config = config;
    _pjn.set_projection(_config);

    _prescale = 2.0;
}
/// @brief Function to convert ISO time string to time_t in UTC
/// @param isoTime ISO date string
/// @return time_t time stamp
time_t CmlInterp::convertIsoToTime(const std::string& isoTime)
{
    std::tm tm = {};
    strptime(isoTime.c_str(), "%Y-%m-%dT%H:%M:%SZ", &tm);
    tm.tm_gmtoff = 0L;
    std::time_t result = timegm(&tm);
    return result;
}
/// @brief Function to return an ISO string from a time_t
/// @param ts time_t time stamp assumed to be UTC
/// @return std::string with ISO date format
std::string CmlInterp::convertTimeToIso(const time_t ts)
{
    char temp[100] = { 0 };
    strftime(temp, 100, "%Y-%m-%dT%H:%M:%SZ", gmtime(&ts));
    return std::string(temp);
}
/// @brief Read the link metadata for the links in the domain
/// @return Number of links that have been found
int CmlInterp::get_link_ids()
{
    mongocxx::database db = _client->database("cml");
    mongocxx::collection cml_metadata = db.collection("cml_metadata");

    double c_lat = _config["domain"]["centre_lat"].get<double>();
    double c_lon = _config["domain"]["centre_lon"].get<double>();
    int n_rows = _config["domain"]["n_rows"].get<int>();
    int n_cols = _config["domain"]["n_cols"].get<int>();
    int p_size = _config["domain"]["p_size"].get<int>();

    double range = sqrt(pow(n_rows * p_size / 2.0, 2.0) + pow(n_cols * p_size / 2.0, 2.0));

    bsoncxx::builder::stream::document query_builder;
    query_builder << "properties.midpoint" << bsoncxx::builder::stream::open_document
                  << "$nearSphere" << bsoncxx::builder::stream::open_document << "$geometry"
                  << bsoncxx::builder::stream::open_document << "type" << "Point"
                  << "coordinates" << bsoncxx::builder::stream::open_array << c_lon << c_lat
                  << bsoncxx::builder::stream::close_array
                  << bsoncxx::builder::stream::close_document << "$maxDistance" << range
                  << bsoncxx::builder::stream::close_document
                  << bsoncxx::builder::stream::close_document;

    auto query = query_builder.view();

    _link_coordinates.clear();
    try {
        auto cursor = cml_metadata.find(query);
        for (const auto& doc : cursor) {
            try {
                auto properties = doc["properties"].get_document().view();
                if (!properties["link_id"] || !properties["midpoint"])
                    continue;

                int link_id = properties["link_id"].get_int32();
                auto midpoint = properties["midpoint"].get_document().view();
                auto coordinates = midpoint["coordinates"].get_array().value;

                double link_lon = coordinates[0].get_double();
                double link_lat = coordinates[1].get_double();
                double link_x { 0 }, link_y { 0 };

                _pjn.to_image_coords(link_lon, link_lat, link_x, link_y);
                _link_coordinates[link_id] = { link_lon, link_lat, link_x, link_y };

            } catch (const std::exception& e) {
                std::cerr << "Error processing document: " << e.what() << std::endl;
            }
        }
    } catch (const mongocxx::query_exception& e) {
        std::cerr << "Query failed: " << e.what() << std::endl;
        _link_coordinates.clear();
    }

    return _link_coordinates.size();
}

/// @brief Generate the rainfall map using ordinary Kriging 
/// @param m_time valid time for the map
Eigen::MatrixXf CmlInterp::make_map_ok(time_t m_time)
{
    // get the link rain for this time
    auto link_rain = get_link_rain(m_time);
    std::cout << std::format("Found {} links with data", link_rain.size()) << std::endl;

    Kriging krig;
    krig.set_params(10, 15.0, 1.0); // default params range in pixel units 

    int n_rows = (int)_config["domain"]["n_rows"].get<int>();
    int n_cols = (int)_config["domain"]["n_cols"].get<int>();

    // Search for a new set of links at box_step intervals and use these within the box
    int box_step = 5; // needs to be an odd number
    int dbox = (int)(box_step / 2.0);
    float range = 20; // distance in image coords 
    std::vector<Observations> local_obs;
    int min_number_locals = 10;

    Eigen::MatrixXf map(n_rows, n_cols);

    // loop over the boxes
    for (float row = dbox; row < n_rows; row += box_step) {
        for (float col = dbox; col < n_cols; col += box_step) {

            // Get the observations for within range of the center of the box
            local_obs.clear();
            for (const auto& link : link_rain) {
                float dy = link.y - row;
                float dx = link.x - col;
                float dist = sqrt(dx * dx + dy * dy);
                if (dist < range) {
                    Observations obs;
                    obs.value = link.value;
                    obs.x = link.x;
                    obs.y = link.y;
                    local_obs.push_back(obs);
                }
            }
            int number_locals = local_obs.size();

            // Do the interpolation if we have enough locals
            if (number_locals >= min_number_locals) {
                Eigen::MatrixXd gamma = krig.buildGammaMatrix(local_obs);
                Eigen::VectorXd values(number_locals);

                // Interpolate within the box
                for (float ia = -dbox; ia <= dbox; ia++) {
                    for (int ib = -dbox; ib <= dbox; ib++) {
                        int y = (int)(row + ia);
                        int x = (int)(col + ib);
                        if (y >= 0 && y < n_rows && x >= 0 && x < n_cols) {

                            // calculate the weights
                            for (std::size_t iobs = 0; iobs < local_obs.size(); iobs++) {
                                double dx = x - local_obs[iobs].x;
                                double dy = y - local_obs[iobs].y;
                                double dist = sqrt(dx * dx + dy * dy);
                                double gamma = krig.variogram(dist);
                                values(iobs) = gamma;
                            }
                            Eigen::VectorXd weights = krig.solveWeights(gamma, values);

                            double val = 0;
                            for (int ival = 0; ival < number_locals; ival++) {
                                val += local_obs[ival].value * weights(ival);
                            }

                            // check the limits for the rain value
                            if (val > 200)
                                val = NAN;
                            if (val < 0.5)
                                val = 0.0;

                            map(y, x) = val;
                        }
                    }
                }
            }

            // not enough locals so fill with nan
            else {
                for (float ia = -dbox; ia <= dbox; ia++) {
                    for (int ib = -dbox; ib <= dbox; ib++) {
                        int y = (int)(row + ia);
                        int x = (int)(col + ib);
                        if (y >= 0 && y < n_rows && x >= 0 && x < n_cols) {
                            map(y, x) = NAN;
                        }
                    }
                }
            }
        }
    }
    return map;
}

/// @brief Generate the rainfall map using Inverse Distance Weighting 
/// @param m_time valid time for the map
Eigen::MatrixXf CmlInterp::make_map_idw(time_t m_time)
{
    // get the link rain for this time
    auto link_rain = get_link_rain(m_time);
    std::cout << std::format("Found {} links with data", link_rain.size()) << std::endl;

    int n_rows = (int)_config["domain"]["n_rows"].get<int>();
    int n_cols = (int)_config["domain"]["n_cols"].get<int>();

    // Search for a new set of links at box_step intervals and use these within the box
    int box_step = 5; // needs to be an odd number
    int dbox = (int)(box_step / 2.0);
    float range = 20000 / _pjn.delta(); // distance in image coords
    std::vector<Observations> local_obs;
    int min_number_locals = 10;

    Eigen::MatrixXf map(n_rows, n_cols);

    // loop over the boxes
    for (float row = dbox; row < n_rows; row += box_step) {
        for (float col = dbox; col < n_cols; col += box_step) {

            // Get the observations for within range of the center of the box
            local_obs.clear();
            for (const auto& link : link_rain) {
                float dy = link.y - row;
                float dx = link.x - col;
                float dist = sqrt(dx * dx + dy * dy);
                if (dist < range) {
                    Observations obs;
                    obs.value = link.value;
                    obs.x = link.x;
                    obs.y = link.y;
                    local_obs.push_back(obs);
                }
            }
            int number_locals = local_obs.size();

            // Do the interpolation if we have enough locals
            if (number_locals >= min_number_locals) {
                Eigen::VectorXd weights(number_locals);

                // Interpolate within the box
                for (float ia = -dbox; ia <= dbox; ia++) {
                    for (int ib = -dbox; ib <= dbox; ib++) {
                        int y = (int)(row + ia);
                        int x = (int)(col + ib);
                        if (y >= 0 && y < n_rows && x >= 0 && x < n_cols) {

                            double val = 0;
                            double sum_weight = 0;
                            for (std::size_t iobs = 0; iobs < number_locals; iobs++) {
                                double dx = x - local_obs[iobs].x;
                                double dy = y - local_obs[iobs].y;
                                double weight = 1.0/(dx * dx + dy * dy);
                                sum_weight += weight; 
                                val += weight *  local_obs[iobs].value ;
                            }
                            val /= sum_weight ; 
                            
                            // check the limits for the rain value
                            if (val > 200)
                                val = NAN;
                            if (val < 0.5)
                                val = 0.0;

                            map(y, x) = val;
                        }
                    }
                }
            }

            // not enough locals so fill with nan
            else {
                for (float ia = -dbox; ia <= dbox; ia++) {
                    for (int ib = -dbox; ib <= dbox; ib++) {
                        int y = (int)(row + ia);
                        int x = (int)(col + ib);
                        if (y >= 0 && y < n_rows && x >= 0 && x < n_cols) {
                            map(y, x) = NAN;
                        }
                    }
                }
            }
        }
    }
    return map;
}

/// @brief Read the link rainfall data
/// @param m_time Valid time
/// @return unordered map with link_id and rain amount
std::vector<Observations> CmlInterp::get_link_rain(time_t m_time)
{
    std::vector<Observations> link_rain;

    mongocxx::database db = _client->database("cml");
    mongocxx::collection cml_data = db.collection("cml_data");

    const auto time_tp = std::chrono::system_clock::from_time_t(m_time);

    // Build the array of link ids
    bsoncxx::builder::stream::array array_builder;
    for (const auto& link : _link_coordinates) {
        int link_id = link.first;
        array_builder << link_id;
    }

    // Search for all link_ids in the domain for m_time and with a rain key:value pair
    bsoncxx::builder::stream::document query_builder;
    query_builder << "link_id" << bsoncxx::builder::stream::open_document << "$in"
                  << array_builder.view() << bsoncxx::builder::stream::close_document
                  << "time.end_time" << bsoncxx::types::b_date(time_tp) << "rain"
                  << bsoncxx::builder::stream::open_document << "$exists" << true
                  << bsoncxx::builder::stream::close_document;

    auto query = query_builder.view();

    try {
        auto cursor = cml_data.find(query);
        for (auto&& doc : cursor) {
            try {
                if (doc["link_id"] && doc["rain"]) {
                    int link_id = doc["link_id"].get_int32();
                    double val = doc["rain"].get_double();
                    link_rain.push_back(
                        { val, _link_coordinates[link_id].x, _link_coordinates[link_id].y });
                }

            } catch (const bsoncxx::exception& e) {
                std::cerr << "BSON parsing error: " << e.what() << std::endl;
                continue;
            }
        }

    } catch (const mongocxx::query_exception& e) {
        std::cerr << "Query execution error: " << e.what() << std::endl;
    }

    return link_rain;
}

void CmlInterp::writeNetCDF(
    const std::string& filename, const Eigen::MatrixXf& data, time_t map_time)
{
    // Get grid coords
    std::vector<float> x = _pjn.x_vals();
    std::vector<float> y = _pjn.y_vals();
    int nx = _pjn.nx();
    int ny = _pjn.ny();
    int nt = 1;

    // Create NetCDF file
    netCDF::NcFile file(filename, netCDF::NcFile::replace);

    // Define dimensions
    auto xDim = file.addDim("x", nx);
    auto yDim = file.addDim("y", ny);
    auto tDim = file.addDim("time", nt);

    // Define variables
    auto xVar = file.addVar("x", netCDF::ncFloat, xDim);
    auto yVar = file.addVar("y", netCDF::ncFloat, yDim);
    auto tVar = file.addVar("time", netCDF::ncInt64, tDim);
    auto dataVar = file.addVar("rainfall", netCDF::ncFloat, { tDim, yDim, xDim });

    // Add CF-compliant attributes to coordinate variables
    xVar.putAtt("standard_name", "projection_x_coordinate");
    xVar.putAtt("units", "m");

    yVar.putAtt("standard_name", "projection_y_coordinate");
    yVar.putAtt("units", "m");

    tVar.putAtt("standard_name", "time");
    tVar.putAtt("units", "seconds since 1970-01-01T00:00:00Z");
    tVar.putAtt("calendar", "gregorian");

    dataVar.putAtt("units", "mm/hr");
    dataVar.putAtt("long_name", "Interpolated rainfall rate");
    dataVar.putAtt("grid_mapping", "projection");

    // Add CF-compliant projection variable
    float lon_0 = _config["description"]["projection"]["central_meridian"].get<float>();
    float lat_0 = _config["description"]["projection"]["latitude_of_origin"].get<float>();
    float east = _config["description"]["projection"]["false_easting"].get<float>();
    float north = _config["description"]["projection"]["false_northing"].get<float>();
    float sma = 6378137.0; // Semi-major axis of the WGS84 ellipsoid
    float flat = 298.257222101; // Inverse flattening of the WGS84 ellipsoid

    auto projVar = file.addVar("projection", netCDF::ncByte);
    projVar.putAtt("grid_mapping_name", "lambert_azimuthal_equal_area");
    projVar.putAtt("longitude_of_projection_origin", netCDF::ncFloat, lon_0);
    projVar.putAtt("latitude_of_projection_origin", netCDF::ncFloat, lat_0);
    projVar.putAtt("false_easting", netCDF::ncFloat, east);
    projVar.putAtt("false_northing", netCDF::ncFloat, north);
    projVar.putAtt("semi_major_axis", netCDF::ncFloat, sma);
    projVar.putAtt("inverse_flattening", netCDF::ncFloat, flat);

    // Add the EPSG name for convenience
    std::string name = _config["crs"]["properties"]["name"].get<std::string>();
    projVar.putAtt("name", name);

    // Write data to variables
    xVar.putVar(x.data());
    yVar.putVar(y.data());
    tVar.putVar(&map_time);

    // Flatten data for writing
    std::vector<float> flatData(data.size());
    for (Eigen::Index i = 0; i < data.rows(); ++i) {
        for (Eigen::Index j = 0; j < data.cols(); ++j) {
            flatData[i * data.cols() + j] = static_cast<float>(data(i, j));
        }
    }
    dataVar.putVar(flatData.data());
}
