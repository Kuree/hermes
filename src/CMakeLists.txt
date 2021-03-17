add_library(hermes event.hh plasma.cc process.cc util.cc)
target_link_libraries(hermes arrow::arrow arrow::plasma fmt::fmt)
target_include_directories(hermes PRIVATE ${PYARROW_INCLUDE_DIR}
        ../extern/cpp-subprocess
        ../extern/fmt/include)
