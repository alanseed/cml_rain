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

/// @brief Set up the MongoDB client manager for this class
CmlInterp::CmlInterp() { _client = &MongoClientManager::get_client(); }
/// @brief Set up the map domain
/// @param config JSON configuration
void CmlInterp::set_config(json config)
{
    _config = config;
    _pjn.set_projection(_config);
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
}
/// @brief Read the link rainfall data
/// @param m_time Valid time
/// @return unordered map with link_id and rain amount
std::unordered_map<int, double> CmlInterp::get_link_rain(time_t m_time)
{
    std::unordered_map<int, double> link_rain;

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
                    link_rain[link_id] = val;
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
