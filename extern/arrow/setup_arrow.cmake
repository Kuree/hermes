# we need to deal with arrow
# most of the stuff came from https://github.com/cylondata/cylon/
include(ExternalProject)
set(ARROW_HOME ${CMAKE_BINARY_DIR}/arrow/install)
set(ARROW_ROOT ${CMAKE_BINARY_DIR}/arrow)
set(PARQUET_ARGS " -DARROW_WITH_BROTLI=OFF"
        " -DARROW_WITH_SNAPPY=ON"
        " -DARROW_WITH_ZLIB=OFF")

set(ARROW_CMAKE_ARGS " -DARROW_WITH_LZ4=OFF"
        " -DARROW_BUILD_SHARED=OFF"
        " -DARROW_WITH_ZSTD=OFF"
        " -DARROW_BUILD_STATIC=ON"
        " -DARROW_BUILD_TESTS=OFF"
        " -DARROW_TEST_MEMCHECK=OFF"
        " -DARROW_BUILD_BENCHMARKS=OFF"
        " -DARROW_DEPENDENCY_SOURCE=BUNDLED"
        " -DARROW_IPC=OFF"
        " -DARROW_COMPUTE=OFF"
        " -DARROW_PLASMA=OFF"
        " -DARROW_FLIGHT=OFF"
        " -DARROW_USE_GLOG=OFF"
        " -DARROW_BUILD_UTILITIES=OFF"
        " -DARROW_HDFS=OFF"
        " -DARROW_S3=ON"
        " -DARROW_TENSORFLOW=OFF"
        " -DARROW_FILESYSTEM=ON"
        " -DARROW_JEMALLOC=OFF"
        " -DARROW_WITH_RE2=OFF"
        " -DARROW_BOOST_USE_SHARED=OFF"
        " -DORC_SOURCE=BUNDLED"
        " -DARROW_WITH_UTF8PROC=OFF"
        " -DARROW_PARQUET=ON"
        " -DARROW_THRIFT=OFF"
        ${PARQUET_ARGS}
        )

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/arrow.CMakeLists.txt.cmake"
        "${ARROW_ROOT}/CMakeLists.txt")

file(MAKE_DIRECTORY "${ARROW_ROOT}/build")
file(MAKE_DIRECTORY "${ARROW_ROOT}/install")


execute_process(
        COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -E env \"CXXFLAGS=-static-libgcc -static-libstdc++\" .
        RESULT_VARIABLE ARROW_CONFIG
        WORKING_DIRECTORY ${ARROW_ROOT})

execute_process(
        COMMAND ${CMAKE_COMMAND} --build .. -- -j 8
        RESULT_VARIABLE ARROW_BUILD
        WORKING_DIRECTORY ${ARROW_ROOT}/build)
unset(ENV{LDFLAGS})
