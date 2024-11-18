#ifndef CML_INTERP_H
#define CML_INTERP_H
#include <ctime>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
using json = nlohmann::json;

// Include the Singleton header for the mongodb client 
#include "mongo_client_manager.h" 

class CmlInterp {
public:
    CmlInterp(); 
    time_t convertIsoToTime(const std::string& isoTime);
    std::string convertTimeToIso(const time_t ts);
    std::vector<int> get_link_ids(json config);

private:
    mongocxx::client* client;  
};

#endif // CML_INTERP_H
