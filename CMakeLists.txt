cmake_minimum_required(VERSION 3.6.0)
project(cml_rain VERSION 0.1.0) 
enable_language(C)
enable_language(CXX)

# require C++17 or better, disable compiler extensions by default
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# set a high warning level
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pedantic -Wextra -Wno-unused-parameter")

find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)
find_package(netCDF  REQUIRED)
find_package(UDUNITS2 REQUIRED)
find_package(PROJ CONFIG REQUIRED)

find_package(libmongocxx REQUIRED)
find_package(libbsoncxx REQUIRED)
include_directories(${LIBMONGOCXX_INCLUDE_DIR})
include_directories(${LIBBSONCXX_INCLUDE_DIR})
include_directories("/usr/local/include/mongocxx/v_noabi")
include_directories("/usr/local/include/bsoncxx/v_noabi") 

find_package(GSL REQUIRED)

