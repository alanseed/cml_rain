#ifndef CML_INTERP_H
#define CML_INTERP_H
#include <ctime>
#include <string>
#include <vector>
#include <unordered_map>

#include <nlohmann/json.hpp>
using json = nlohmann::json;

// Include the Singleton header for the mongodb client
#include "mongo_client_manager.h"
#include "image_projection.h"
/// @brief Structure for link coordinates 
struct Coordinates {
    double lon;
    double lat;
    double x;
    double y;
};

class CmlInterp {
public:
    CmlInterp();
    time_t convertIsoToTime(const std::string& isoTime);
    std::string convertTimeToIso(const time_t ts);
    void set_config(json config);
    int get_link_ids();
    void make_map(time_t m_time);
    std::unordered_map<int, double> get_link_rain(time_t m_time); 
    image_projection _pjn; 

private:
    mongocxx::client* _client;
    std::unordered_map<int, Coordinates> _link_coordinates;    
    json _config;

};

#endif // CML_INTERP_H
