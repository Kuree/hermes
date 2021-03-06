cmake_minimum_required(VERSION 3.8)
project(hermes)
set(CMAKE_CXX_STANDARD 17)

# add debug flag so that slang won't trigger seg fault
if (CMAKE_BUILD_TYPE MATCHES "Debug")
    add_compile_definitions(DEBUG)
endif()

# find these packages before arrow
set(OPENSSL_USE_STATIC_LIBS TRUE)
find_package(OpenSSL REQUIRED)
find_package(CURL REQUIRED)


# set options
option(PERFORMANCE_TEST "Build performance test programs" OFF)

# external modules
add_subdirectory(extern)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")
find_package(Arrow REQUIRED)
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)#

# only enabled when we're interested in performance test
if (PERFORMANCE_TEST)
    add_compile_definitions("PERFORMANCE_TEST")
endif()

add_subdirectory(src)

# tests
add_subdirectory(extern/googletest)
include(GoogleTest)
include (CTest)
enable_testing()
add_subdirectory(tests)
