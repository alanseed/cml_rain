#include <netcdf>
#include <vector>
#include <Eigen/Dense>

void writeNetCDF(const std::string& filename,
                 const std::vector<double>& lat,
                 const std::vector<double>& lon,
                 const Eigen::MatrixXd& data) {
    const int nLat = lat.size();
    const int nLon = lon.size();

    netCDF::NcFile file(filename, netCDF::NcFile::replace);

    // Define dimensions
    auto latDim = file.addDim("lat", nLat);
    auto lonDim = file.addDim("lon", nLon);

    // Define variables
    auto latVar = file.addVar("lat", netCDF::ncDouble, latDim);
    auto lonVar = file.addVar("lon", netCDF::ncDouble, lonDim);
    auto dataVar = file.addVar("rainfall", netCDF::ncDouble, {latDim, lonDim});

    // Add attributes (CF-compliance)
    latVar.putAtt("units", "degrees_north");
    lonVar.putAtt("units", "degrees_east");
    dataVar.putAtt("units", "mm/hr");
    dataVar.putAtt("long_name", "Interpolated rainfall rate");

    // Write data
    latVar.putVar(lat.data());
    lonVar.putVar(lon.data());
    dataVar.putVar(data.data());
}
