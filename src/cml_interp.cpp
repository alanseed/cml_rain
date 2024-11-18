#include "cml_interp.h"

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/database.hpp>
#include <mongocxx/exception/query_exception.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

CmlInterp::CmlInterp() { client = &MongoClientManager::get_client(); }

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

// Search the metadata for the link_ids that are inside the map area
std::vector<int> CmlInterp::get_link_ids(json config)
{
    mongocxx::database db = client->database("cml");
    mongocxx::collection cml_metadata = db.collection("cml_metadata");

    // Unpack the config file
    double c_lat = config["c_lat"];
    double c_lon = config["c_lon"];
    int n_rows = config["n_rows"];
    int p_size = config["p_size"];

    // Assume a square domain for now
    double range = sqrt(2.0) * n_rows * p_size / 2.0;

    // Build the geospatial query
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

    // Vector to store the link IDs
    std::vector<int> links;

    try {
        // Execute the query and iterate through the results
        auto cursor = cml_metadata.find(query);
        for (auto&& doc : cursor) {
            auto properties = doc["properties"].get_document().view();

            // Safely retrieve the link_id if it exists
            if (properties["link_id"]) {
                int link_id = properties["link_id"].get_int32();
                links.push_back(link_id);
            }
        }
    } catch (const mongocxx::query_exception& e) {
        std::cerr << "Query failed: " << e.what() << std::endl;
        // Optionally return an empty vector or handle the failure gracefully
    }
    return links;
}