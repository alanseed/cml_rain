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

#include "kriging.h"
#include <cassert>
#include <cmath> // for HUGE_VAL
#include <iomanip> // for std::setprecision()
#include <iostream>

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

    double c_lat = _config["c_lat"];
    double c_lon = _config["c_lon"];
    int n_rows = _config["n_rows"];
    int n_cols = _config["n_cols"];
    int p_size = _config["p_size"];

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

/// @brief Generate the rainfall map
/// @param m_time valid time for the map
void CmlInterp::make_map(time_t m_time)
{
    // get the link rain for this time
    auto link_rain = get_link_rain(m_time);
    std::cout << std::format("Found {} links with data", link_rain.size()) << std::endl;

    Kriging krig;

    int n_rows = (int)_config["n_rows"];
    int n_cols = (int)_config["n_cols"];

    // Search for a new set of links at box_step intervals and use these within the box
    int box_step = 5; // needs to be an odd number
    int dbox = (int)(box_step / 2.0);
    float range = 30000;
    std::vector<Observations> local_obs;
    int min_number_locals = 5;

    Eigen::MatrixXf map(n_rows, n_cols);
    
    // loop over the boxes 
    for (float row = dbox; row < n_rows; row += box_step) {
        for (float col = dbox; col < n_cols; col += box_step) {

            // Get the observations for within range of the center of the box
            local_obs.clear();
            for (int ia = 0; ia < link_rain.size(); ia++) {
                float dy = row - link_rain[ia].y;
                float dx = col - link_rain[ia].x;
                float dist = sqrt(dx * dx + dy * dy);
                if (dist < range) {
                    local_obs.push_back(link_rain[ia]);
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
                            for (int iobs = 0; iobs < local_obs.size(); iobs++) {
                                double dx = x - local_obs[iobs].x;
                                double dy = y - local_obs[iobs].y;
                                double dist = sqrt(dx * dx + dy * dy);
                                values(iobs) = krig.variogram(dist);
                            }
                            Eigen::VectorXd weights = krig.solveWeights(gamma, values);

                            double val = 0;
                            for (int ival = 0; ival < number_locals; ival++) {
                                val += local_obs[ival].value * weights(ival);
                            }

                            // transform back into rain rate
                            map(y, x) = from_ihs(val);
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
                    double tval = to_ihs(val);

                    link_rain.push_back(
                        { tval, _link_coordinates[link_id].x, _link_coordinates[link_id].y });
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

#if 0
// write the output images as a netcdf file
// one time step per file
void grids::write_grids(std::string file_name)
{
    short int _fillValue = std::numeric_limits<short int>::max();

    int compress = 6;
    int nt = 1;
    int ny = projection_.ny();
    int nx = projection_.nx();
    auto x_vals = projection_.x_vals();
    auto y_vals = projection_.y_vals();

    unsigned long int chunk[3] = {(unsigned long)nt, (unsigned long)ny, (unsigned long)nx};
    steps::nc::file file(file_name, steps::nc::io_mode::create);

    // create the dimensions
    auto &time = file.create_dimension("time", nt);
    auto &dy = file.create_dimension("y", ny);
    auto &dx = file.create_dimension("x", nx);

    // now the variables
    auto &dv =
        file.create_variable("valid_time", steps::nc::data_type::u64, {&time});
    dv.att_set("standard_name", "time");
    dv.att_set("units", "seconds since 1970-01-01 00:00:00 UTC");
    dv.att_set("long_name", "Valid UTC time");
    dv.write(steps::span{&valid_time_, 1});

    auto &vy = file.create_variable("y", steps::nc::data_type::f32, {&dy});
    vy.att_set("standard _name", "projection_y_coordinate");
    vy.att_set("long_name", "projection_y_coordinate");
    vy.att_set("units", "m");
    vy.write(steps::span{y_vals.data(), (int)y_vals.size()});

    auto &vx = file.create_variable("x", steps::nc::data_type::f32, {&dx});
    vx.att_set("standard_name", "projection_x_coordinate");
    vx.att_set("long_name", "projection_x_coordinate");
    vx.att_set("units", "m");
    vx.write(steps::span{x_vals.data(), (int)x_vals.size()});

    auto &proj = file.create_variable("proj", steps::nc::data_type::u8);
    proj.att_set("grid_mapping_name", "albers_conical_equal_area");
    float std_par[2] = {projection_.lat_1(), projection_.lat_2()};
    proj.att_set("standard_parallel", steps::span{std_par, 2});
    proj.att_set("longitude_of_central_meridian", projection_.lon_0());
    proj.att_set("latitude_of_projection_origin", projection_.lat_0());
    proj.att_set("false_easting", projection_.false_east());
    proj.att_set("false_northing", projection_.false_north());
    double towgs84[7] = {0.0};
    proj.att_set("towgs84", steps::span{towgs84, 7});

    // loop over the output images and write them out
    for (int igrid = 0; igrid < (int)grid_data_.size(); ++igrid)
    {
        // set up the output variable for the grid data
        auto &vp = file.create_variable(variable_[igrid], steps::nc::data_type::i16, {&time, &dy, &dx}, {chunk[0], chunk[1], chunk[2]}, compress);
        vp.att_set("grid_mapping", "proj");
        vp.att_set("long_name", long_name_[igrid]);
        vp.att_set("standard_name", standard_name_[igrid]);
        vp.att_set("units", units_[igrid]);
        vp.att_set("add_offset", add_offset_[igrid]);
        vp.att_set("scale_factor", scale_factor_[igrid]);
        vp.att_set("_FillValue", _fillValue);

        // rescale and set the fill values then write out the grid data
        std::vector<short int> out_data(ny * nx);
        auto *in_dataP = grid_data_[igrid].data();
        auto *out_dataP = out_data.data();
        for (auto ia = 0; ia < ny * nx; ++ia)
        {
            if (std::isnan(in_dataP[ia]))
            {
                out_dataP[ia] = _fillValue;
            }
            else
            {
                out_dataP[ia] = (short int)((in_dataP[ia] - add_offset_[igrid]) / scale_factor_[igrid]);
            }
        }
        vp.write(steps::span{out_data.data(), (long int)(ny * nx)});
    }

    // write out the global attributes
    file.att_set("station_id", projection_.radar_id());
    file.att_set("Conventions", "CF-1.7");
}

// Read in a list of variables from a nc file
// For now assume that the grids in the nc file have the same projection etc as the standard
// rf3 projection in the mongo database and add the grids to the vector of grids already in the class
void grids::read_grids(std::string file_name, std::vector<std::string> var_list)
{
    steps::nc::file file(file_name, steps::nc::io_mode::read_only);

    // read in the station id for this file and set the projection
    int station_id;
    file.att_get("station_id", station_id);
    projection_.set_projection(station_id);

    auto nx = projection_.nx();
    auto ny = projection_.ny();

    // read in the valid time
    bool valid_time_exists = file.att_exists("valid_time");
    if (valid_time_exists)
        file.lookup_variable("valid_time").read(steps::span{&valid_time_, 1});
    else
        valid_time_ = 0;

    // loop over the list of variables to be read in
    for (auto ivar = 0; ivar < (int)var_list.size(); ++ivar)
    {
        std::string variable = var_list[ivar];
        std::string long_name{' '};
        std::string standard_name{' '};
        std::string units{' '};
        float scale_factor{1.0};
        float add_offset{0.0};
        double _FillValue{NAN};

        auto nc_variable = file.find_variable(variable);
        if (nc_variable->att_exists("long_name"))
            file.find_variable(variable)->att_get("long_name", long_name);
        if (nc_variable->att_exists("standard_name"))
            file.find_variable(variable)->att_get("standard_name", standard_name);
        if (nc_variable->att_exists("units"))
            file.find_variable(variable)->att_get("units", units);
        if (nc_variable->att_exists("scale_factor"))
            file.find_variable(variable)->att_get("scale_factor", scale_factor);
        if (nc_variable->att_exists("add_offset"))
            file.find_variable(variable)->att_get("add_offset", add_offset);
        if (nc_variable->att_exists("_FillValue"))
            file.find_variable(variable)->att_get("_FillValue", _FillValue);

        std::vector<short int> file_data(ny * nx, 0);
        long int len = file_data.size();
        file.lookup_variable(variable).read(steps::span{file_data.data(), len});

        std::vector<float> var_data(nx * ny, 0);
        for (auto i = 0; i < ny * nx; ++i)
        {
            if (isnan(double(file_data[i]) || file_data[i] == _FillValue))
            {
                var_data[i] = NAN;
            }
            else
            {
                var_data[i] = file_data[i] * scale_factor + add_offset;
            }
        }

        grid_data_.push_back(var_data);
        long_name_.push_back(long_name);
        standard_name_.push_back(standard_name);
        units_.push_back(units);
        scale_factor_.push_back(scale_factor);
        add_offset_.push_back(add_offset);
        variable_.push_back(variable);
    }
}
#endif