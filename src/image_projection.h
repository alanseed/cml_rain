// class to convert from lon,lat to img_x,img_y
// origin in SW corner of the field
// units in pixels
#include <nlohmann/json.hpp>
#include <proj.h>
#include <vector>
using json = nlohmann::json;

class image_projection {
private:
    PJ_CONTEXT* _ctx;
    PJ* _trans_proj;
    int _nx; // Number of columns in the grid
    int _ny; // Number of rows in the grid
    float _start_x; // SW corner
    float _start_y; // SW corner
    float _delta; // pixel size in m
    std::string _pjn; // EPSG string

public:
    image_projection();
    ~image_projection();
    void set_projection(json config);
    void to_image_coords(double lon, double lat, double& ix, double& iy);
    void from_image_coords(double& lon, double& lat, double im_x, double im_y);
    int nx() { return _nx; };
    int ny() { return _ny; };
    float start_x() { return _start_x; };
    float start_y() { return _start_y; };
    float delta() { return _delta; };

    std::vector<float> x_vals();
    std::vector<float> y_vals();

    image_projection& operator=(const image_projection& r)
    {
        if (this == &r)
            return *this;

        _nx = r._nx;
        _ny = r._ny;
        _start_x = r._start_x;
        _start_y = r._start_y;
        _delta = r._delta;
        _pjn = r._pjn;
        return *this;
    }

    bool operator==(const image_projection& r)
    {
        float epsilon = 0.01f;
        if (_nx != r._nx)
            return false;
        if (_ny != r._ny)
            return false;
        if (fabs(_start_x - r._start_x) > epsilon)
            return false;
        if (fabs(_start_y - r._start_y) > epsilon)
            return false;
        if (fabs(_delta - r._delta) > epsilon)
            return false;
        if (!(_pjn == r._pjn))
            return false;

        return true;
    }
};
