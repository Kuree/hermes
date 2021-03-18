add_library(hermes event.cc plasma.cc process.cc util.cc)
target_link_libraries(hermes arrow::arrow arrow::plasma arrow::parquet fmt::fmt)
target_include_directories(hermes PRIVATE ${PYARROW_INCLUDE_DIR}
        ../extern/cpp-subprocess
        ../extern/fmt/include)
add_compile_definitions(_GLIBCXX_USE_CXX11_ABI=0)
