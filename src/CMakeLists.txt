add_library(hermes event.cc)
target_link_libraries(hermes arrow::arrow arrow::plasma)
target_include_directories(hermes PRIVATE ${PYARROW_INCLUDE_DIR})