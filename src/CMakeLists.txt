add_library(hermes event.cc process.cc util.cc transaction.cc arrow.cc serializer.cc loader.cc tracker.cc
        pubsub.cc logger.cc query.cc checker.cc rtl.cc)
# the ordering of linked libraries is very important! since the linker will discard unused functions in processing
# order
target_link_libraries(hermes arrow::parquet arrow::thrift fmt::fmt arrow::arrow arrow::snappy slang::compiler)
target_include_directories(hermes PRIVATE ${ARROW_INCLUDE_DIR} ${SLANG_INCLUDE_DIR}
        ../extern/cpp-subprocess
        ../extern/fmt/include
        ../extern/rapidjson/include)
set_property(TARGET hermes PROPERTY POSITION_INDEPENDENT_CODE ON)

add_subdirectory(dpi)
add_subdirectory(python)