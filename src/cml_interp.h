#ifndef CML_INTERP_H
#define CML_INTERP_H
#include <Eigen/Dense>
#include <cmath>
#include <ctime>
#include <string>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>
using json = nlohmann::json;

// Include the Singleton header for the mongodb client
#include "image_projection.h"
#include "mongo_client_manager.h"
/// @brief Structure for link coordinates
struct Coordinates {
    double lon;
    double lat;
    double x;
    double y;
};

/// @brief Structure with link rain data
struct Observations {
    double value;
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

private:
    mongocxx::client* _client;
    std::unordered_map<int, Coordinates> _link_coordinates;
    json _config;

    image_projection _pjn;

    // Inverse Hyperbolic Transformation
    float _prescale;
    double to_ihs(double value) { return asinh(value * _prescale); };
    double from_ihs(double value) { return value > 0.0f ? sinh(value) / _prescale : 0.0f; };
    std::vector<Observations> get_link_rain(time_t m_time);
};

#endif // CML_INTERP_H
