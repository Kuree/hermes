add_library(hermes event.cc process.cc util.cc transaction.cc arrow.cc serializer.cc loader.cc tracker.cc
        pubsub.cc logger.cc query.cc checker.cc rtl.cc json.cc)
# the ordering of linked libraries is very important! since the linker will discard unused functions in processing
# order
target_link_libraries(hermes arrow::parquet arrow::thrift arrow::arrow arrow::snappy aws::aws slangcompiler
        OpenSSL::Crypto ${CURL_LIBRARIES} ${ZLIB_LIBRARIES})
target_include_directories(hermes SYSTEM PUBLIC ${ARROW_INCLUDE_DIR}
        ../extern/cpp-subprocess
        ../extern/fmt/include
        ../extern/rapidjson/include
        ../extern/slang/include
        ../extern/slang/external/
        ${CMAKE_CURRENT_BINARY_DIR}/../extern/slang/source)
set_property(TARGET hermes PROPERTY POSITION_INDEPENDENT_CODE ON)
target_compile_options(hermes PRIVATE -Wall -Wextra -pedantic -Werror)

add_subdirectory(dpi)
add_subdirectory(python)
add_subdirectory(tools)