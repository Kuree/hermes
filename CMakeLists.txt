cmake_minimum_required(VERSION 3.8)
project(hermes)
set(CMAKE_CXX_STANDARD 17)

# add debug flag so that slang won't trigger seg fault
if (CMAKE_BUILD_TYPE MATCHES "Debug")
    add_compile_definitions(DEBUG)
endif()

# external modules
add_subdirectory(extern)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")
find_package(Arrow REQUIRED)
find_package(Threads REQUIRED)

add_subdirectory(src)

# tests
add_subdirectory(extern/googletest)
include(GoogleTest)
include (CTest)
enable_testing()
add_subdirectory(tests)
