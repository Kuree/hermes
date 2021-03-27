add_library(hermes-dpi dpi.cc)
target_link_libraries(hermes-dpi hermes)
target_include_directories(hermes-dpi PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../)