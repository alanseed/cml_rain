// Alan Seed
// Generate the interpolated maps for the CML rainfall data
// only one connection to the mongo db for the application
#include <chrono>
#include <cxxopts.hpp>
#include <format>
#include <iostream>
#include <nlohmann/json.hpp>
#include <stdio.h>
#include <string>

using json = nlohmann::json;
#include "cml_interp.h"

int run(std::string start, std::string end, json config);
std::vector<int> get_link_ids(json config);

int main(int argc, char* argv[])
{
    cxxopts::Options options(
        "cml_interpolate", "Generate gridded maps from CML rainfall estimates");
    options.add_options()("h,help", "Print usage")(
        "s,start", "Start time as ISO date", cxxopts::value<std::string>())(
        "e,end", "End time as ISO date", cxxopts::value<std::string>());

    auto result = options.parse(argc, argv);
    if (result.count("help")) {
        std::cout << options.help() << std::endl;
        return 0;
    }
    std::string start_str = result["start"].as<std::string>();
    std::string end_str = result["end"].as<std::string>();

    // Set up the map configuration
    json config = { { "name", "n_test" }, { "c_lat", 52.0 }, { "c_lon", 4.0 }, { "n_rows", 300 },
        { "n_cols", 300 }, { "p_size", 1000 }, { "pjn", "EPSG:3035" } };

    auto status = run(start_str, end_str, config);
    return status;
}

int run(std::string start, std::string end, json config)
{
    std::cout << std::format("start date = {}", start) << std::endl;
    std::cout << std::format("end date = {}", end) << std::endl;
    CmlInterp cml;

    // Get the link_ids in the area of interest
    cml.set_config(config) ;
    auto number_links = cml.get_link_ids( );
    std::cout << std::format("Found {} links in map area\n", number_links);

    // Get the start and end times for the maps
    std::time_t start_time = cml.convertIsoToTime(start);
    std::time_t end_time = cml.convertIsoToTime(end);
    int time_step = 15 * 60; // assume 15 min steps
    
    // Loop over the times to be processed 
    for (time_t m_time = start_time; m_time <= end_time; m_time += time_step){
        cml.make_map(m_time); 

    }

    return 0;
}
