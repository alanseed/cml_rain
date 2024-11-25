// Alan Seed
// Generate the interpolated maps for the CML rainfall data
// only one connection to the mongo db for the application
#include <chrono>
#include <cxxopts.hpp>
#include <format>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <stdio.h>
#include <string>
#include <ctime>
#include <filesystem>

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
        "e,end", "End time as ISO date", cxxopts::value<std::string>())(
        "c,config", "Configuration file", cxxopts::value<std::string>());

    auto result = options.parse(argc, argv);
    if (result.count("help")) {
        std::cout << options.help() << std::endl;
        return 0;
    }
    std::string start_str = result["start"].as<std::string>();
    std::string end_str = result["end"].as<std::string>();
    
    // parse the config file 
    std::string config_file = result["config"].as<std::string>();
    std::ifstream f;
    try {
        f.open(config_file, std::ifstream::in);
    } catch (std::ios_base::failure& e) {
        std::cerr << e.what() << '\n';
    }
    json config = json::parse(f);

    // run the application
    auto status = run(start_str, end_str, config);
    return status;
}

int run(std::string start, std::string end, json config)
{
    std::cout << std::format("start date = {}", start) << std::endl;
    std::cout << std::format("end date = {}", end) << std::endl;
    CmlInterp cml;

    // Get the link_ids in the area of interest
    cml.set_config(config);
    auto number_links = cml.get_link_ids();
    std::cout << std::format("Found {} links in map area\n", number_links);

    // Get the start and end times for the maps
    std::time_t start_time = cml.convertIsoToTime(start);
    std::time_t end_time = cml.convertIsoToTime(end);
    int time_step = 15 * 60; // assume 15 min steps

    // Loop over the times to be processed
    std::string data_dir = config["directory"]; 
    std::string name = config["name"];

    for (time_t m_time = start_time; m_time <= end_time; m_time += time_step){
        Eigen::MatrixXf map = cml.make_map_idw(m_time);

        // Format the time as yyyy-mm-ddThh:mm:ss
        char c_time[64] = {0};
        tm *tm_time = gmtime(&m_time); 
        strftime(c_time, 64, "%Y-%m-%dT%H:%M:%S", tm_time);
        
        // Construct the full path for the output file
        std::string full_path = data_dir + std::string(c_time) + "_" + name + ".nc";
        std::cout << std::format("Writing {}\n", full_path); 

        // Write the netCD file 
        cml.writeNetCDF(full_path, map, m_time) ;
    }

    return 0;
}
