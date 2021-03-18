add_library(hermes event.cc plasma.cc process.cc util.cc transaction.cc arrow.cc)
target_link_libraries(hermes arrow::arrow arrow::plasma arrow::parquet fmt::fmt)
target_include_directories(hermes PRIVATE ${ARROW_INCLUDE_DIR}
        ../extern/cpp-subprocess
        ../extern/fmt/include)