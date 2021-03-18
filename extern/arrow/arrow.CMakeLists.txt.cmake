cmake_minimum_required(VERSION 3.10)
project(ArrowModule)
include(ExternalProject)

ExternalProject_Add(Arrow
        GIT_REPOSITORY    https://github.com/apache/arrow.git
        GIT_SHALLOW       TRUE
        GIT_TAG           "apache-arrow-3.0.0"
        SOURCE_DIR        "${ARROW_ROOT}/arrow"
        SOURCE_SUBDIR     "cpp"
        BINARY_DIR        "${ARROW_ROOT}/build"
        INSTALL_DIR       "${ARROW_ROOT}/install"
        CMAKE_ARGS        ${ARROW_CMAKE_ARGS} -DCMAKE_INSTALL_PREFIX=${ARROW_ROOT}/install)