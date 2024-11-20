cmake_minimum_required(VERSION 3.16)
project(cml_interpolate VERSION 0.1.0 LANGUAGES C CXX)

# Require C++20 or better
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Set build type to Debug by default if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# High warning levels
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -Wno-unused-parameter")

# Add CMake module path
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Find required packages
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(PROJ REQUIRED)
find_package(nlohmann_json REQUIRED)

pkg_check_modules(NETCDF REQUIRED netcdf)
pkg_check_modules(LIBMONGOCXX REQUIRED libmongocxx)
pkg_check_modules(LIBBSONCXX REQUIRED libbsoncxx)

# Find Eigen
find_path(EIGEN_INCLUDE_DIR
    NAMES Eigen/Dense
    PATHS /usr/include/eigen3
    NO_DEFAULT_PATH
)

if(NOT EIGEN_INCLUDE_DIR)
    message(FATAL_ERROR "Eigen3 headers not found! Install with: sudo dnf install eigen3-devel")
endif()

# Include directories
include_directories(
    ${LIBMONGOCXX_INCLUDE_DIRS}
    ${LIBBSONCXX_INCLUDE_DIRS}
    ${NETCDF_INCLUDE_DIRS}
    ${EIGEN_INCLUDE_DIR}
)

# Link directories (modern CMake prefers targets, but fallback added)
link_directories(
    ${LIBMONGOCXX_LIBRARY_DIRS}
    ${LIBBSONCXX_LIBRARY_DIRS}
    ${NETCDF_LIBRARY_DIRS}
)

# Source and header files
set(SRC_FILES
    src/main.cpp
    src/cml_interp.cpp
    src/image_projection.cpp
)

set(HDR_FILES
    src/cml_interp.h
    src/mongo_client_manager.h
    src/image_projection.h
    src/kriging.h
    src/write_nc.h
)

# Create the executable
add_executable(cml_interpolate ${SRC_FILES} ${HDR_FILES})

# Link required libraries
target_link_libraries(cml_interpolate
    Threads::Threads
    ZLIB::ZLIB
    PROJ::proj
    ${LIBMONGOCXX_LIBRARIES}
    ${LIBBSONCXX_LIBRARIES}
    ${NETCDF_LIBRARIES}
)

# Ensure include directories are properly set for the target
target_include_directories(cml_interpolate PRIVATE
    ${LIBMONGOCXX_INCLUDE_DIRS}
    ${LIBBSONCXX_INCLUDE_DIRS}
    ${NETCDF_INCLUDE_DIRS}
    ${EIGEN_INCLUDE_DIR}
)
