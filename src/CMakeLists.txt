add_library(hermes event.cc process.cc util.cc transaction.cc arrow.cc serializer.cc loader.cc tracker.cc
        pubsub.cc logger.hh)
target_link_libraries(hermes arrow::arrow arrow::parquet fmt::fmt)
target_include_directories(hermes PRIVATE ${ARROW_INCLUDE_DIR}
        ../extern/cpp-subprocess
        ../extern/fmt/include
        ../extern/rapidjson/include)
set_property(TARGET hermes PROPERTY POSITION_INDEPENDENT_CODE ON)

add_subdirectory(dpi)
