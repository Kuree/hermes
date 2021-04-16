include(ExternalProject)
set(SLANG_HOME ${CMAKE_BINARY_DIR}/slang/install)
set(SLANG_ROOT ${CMAKE_BINARY_DIR}/slang)

set(SLANG_CMAKE_ARGS " -DCMAKE_BUILD_TYPE=Release"
            " -DSLANG_INCLUDE_TESTS=OFF"
            " -DSTATIC_BUILD=ON")

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/slang.CMakeLists.txt.cmake"
        "${SLANG_ROOT}/CMakeLists.txt")

file(MAKE_DIRECTORY "${SLANG_ROOT}/build")
file(MAKE_DIRECTORY "${SLANG_ROOT}/install")


execute_process(
        COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -E env \"CXXFLAGS=-static-libgcc -static-libstdc++\" .
        RESULT_VARIABLE SLANG_CONFIG
        WORKING_DIRECTORY ${SLANG_ROOT})

execute_process(
        COMMAND ${CMAKE_COMMAND} --build .. --target slangcompiler -- -j 4
        RESULT_VARIABLE SLANG_BUILD
        WORKING_DIRECTORY ${SLANG_ROOT}/build)
