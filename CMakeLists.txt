cmake_minimum_required(VERSION 3.8)
project(hermes)
set(CMAKE_CXX_STANDARD 17)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")
find_package(PyArrow REQUIRED)

add_subdirectory(src)

# tests
add_subdirectory(extern/googletest)
include(GoogleTest)
include (CTest)
enable_testing()
add_subdirectory(tests)
