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
    Eigen::MatrixXf make_map_ok(time_t m_time);
    Eigen::MatrixXf make_map_idw(time_t m_time);
    void writeNetCDF(const std::string& filename, const Eigen::MatrixXf& data, time_t map_time);

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

class Kriging {
public:
    Eigen::MatrixXd buildGammaMatrix(const std::vector<Observations>& observations)
    {
        int n = observations.size();
        Eigen::MatrixXd gamma = Eigen::MatrixXd::Zero(n + 1, n + 1);

        for (int i = 0; i < n; ++i) {
            for (int j = 0; j <= i; ++j) {
                double dx = observations[i].x - observations[j].x;
                double dy = observations[i].y - observations[j].y;
                double dist = std::sqrt(dx * dx + dy * dy);
                gamma(i, j) = gamma(j, i) = variogram(dist);
            }
            gamma(i, n) = gamma(n, i) = 1.0;
        }

        gamma(n, n) = 0.0; // Lagrange multiplier
        return gamma;
    }

    Eigen::VectorXd solveWeights(const Eigen::MatrixXd& gamma, const Eigen::VectorXd& values)
    {
        Eigen::VectorXd rhs(values.size() + 1);
        rhs.head(values.size()) = values;
        rhs(values.size()) = 1.0; // Constraint for weights to sum to 1

        // Solve gamma * weights = rhs
        Eigen::VectorXd weights = gamma.ldlt().solve(rhs);
        return weights;
    }

    double variogram(double distance)
    {
        // Example spherical variogram model
        if (distance < 1.0)
            return _nugget;    
        if (distance > _range)
            return _nugget + _sill;
        return _nugget + _sill * (1.5 * distance / _range - 0.5 * std::pow(distance / _range, 3));
    }
    void set_params(double range, double sill, double nugget) { 
        _range = range;
        _sill = sill;
        _nugget = nugget;
    }

private:
    double _range; // pixel units
    double _sill; 
    double _nugget; 
};

#endif // CML_INTERP_H
