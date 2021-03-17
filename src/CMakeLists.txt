add_library(hermes event.hh plasma.cc process.cc)
target_link_libraries(hermes arrow::arrow arrow::plasma)
target_include_directories(hermes PRIVATE ${PYARROW_INCLUDE_DIR}
        ../extern/cpp-subprocess)