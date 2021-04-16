cmake_minimum_required(VERSION 3.10)
project(SlangModule)
include(ExternalProject)

ExternalProject_Add(slangcompiler
        GIT_REPOSITORY    https://github.com/MikePopoloski/slang
        GIT_SHALLOW       TRUE
        GIT_TAG           "v0.7"
        SOURCE_DIR        "${SLANG_ROOT}/slang"
        BINARY_DIR        "${SLANG_ROOT}/build"
        INSTALL_DIR       "${SLANG_ROOT}/install"
        CMAKE_ARGS        ${SLANG_CMAKE_ARGS} -DCMAKE_INSTALL_PREFIX=${SLANG_ROOT}/install)
