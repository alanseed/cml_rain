#include "image_projection.h"
#include <iostream>
#include <format>
/// @brief Constructor
image_projection::image_projection()
{
    _ctx = NULL;
    _trans_proj = NULL;
}
/// @brief Destructor
image_projection::~image_projection()
{
    if (_ctx != NULL)
        proj_context_destroy(_ctx);
    if (_trans_proj != NULL)
        proj_destroy(_trans_proj);
}
/// @brief Initialise the class with the JSON configuration
/// @param config JSON configuration
void image_projection::set_projection(json config)
{
    _nx = config["domain"]["n_cols"].get<int>();
    _ny = config["domain"]["n_rows"].get<int>();
    _delta = config["domain"]["p_size"].get<float>();
    _pjn = config["crs"]["properties"]["name"];
    _ctx = proj_context_create();
    _trans_proj = proj_create_crs_to_crs(_ctx, "EPSG:4326", _pjn.c_str(), NULL);

    double lat = config["domain"]["centre_lat"].get<double>();
    double lon = config["domain"]["centre_lon"].get<double>();

    // none of this makes sense but it seems as though the lam,phi and x,y 
    // coords are swapped around in PJ_COORD
    // see https://epsg.io/transform#s_srs=4326&t_srs=3035&ops=1149&x=NaN&y=NaN 
    // for comparison 

    PJ_COORD a;
    PJ_COORD b;
    a.lp.lam = lat;
    a.lp.phi = lon;
    b = proj_trans(_trans_proj, PJ_FWD, a);
    _start_x = b.xy.y - 0.5 * _nx * _delta;
    _start_y = b.xy.x - 0.5 * _ny * _delta; 
}
/// @brief Convert Lon, Lat into image coords
/// @param lon
/// @param lat
/// @param im_x
/// @param im_y
void image_projection::to_image_coords(double lon, double lat, double& im_x, double& im_y)
{
    // none of this makes sense but it seems as though the lam,phi and x,y 
    // coords are swapped around in PJ_COORD
    // see https://epsg.io/transform#s_srs=4326&t_srs=3035&ops=1149&x=NaN&y=NaN 
    // for comparison 

    PJ_COORD a; 
    PJ_COORD b;
    a.lp.lam = lat;
    a.lp.phi = lon;
    b = proj_trans(_trans_proj, PJ_FWD, a);
    im_x = (b.xy.y - _start_x) / _delta;
    im_y = (b.xy.x - _start_y) / _delta;
}
/// @brief Generate Lon, Lat from image coords
/// @param lon
/// @param lat
/// @param im_x
/// @param im_y
void image_projection::from_image_coords(double& lon, double& lat, double im_x, double im_y)
{
    // none of this makes sense but it seems as though the lam,phi and x,y 
    // coords are swapped around in PJ_COORD
    // see https://epsg.io/transform#s_srs=4326&t_srs=3035&ops=1149&x=NaN&y=NaN 
    // for comparison 

    PJ_COORD a;
    PJ_COORD b;
    a.xy.y = im_x * _delta + _start_x;
    a.xy.x = im_y * _delta + _start_y;
    b = proj_trans(_trans_proj, PJ_INV, a);
    lat = b.lp.lam;
    lon = b.lp.phi;
}
/// @brief Generate the map projection eastings
/// @return
std::vector<float> image_projection::x_vals()
{
    std::vector<float> x_coords(_nx);
    for (int ia = 0; ia < _nx; ++ia) {
        x_coords[ia] = _start_x + ia * _delta;
    }
    return x_coords;
}
/// @brief Generate map projection northings
/// @return
std::vector<float> image_projection::y_vals()
{
    std::vector<float> y_coords(_ny);
    for (int ia = 0; ia < _ny; ++ia) {
        y_coords[ia] = _start_y + ia * _delta;
    }
    return y_coords;
}
