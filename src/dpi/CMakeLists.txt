add_library(hermes-dpi SHARED dpi.cc)
target_link_libraries(hermes-dpi hermes -static-libgcc -static-libstdc++)
target_include_directories(hermes-dpi PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../
        ${CMAKE_CURRENT_SOURCE_DIR}/../../extern/vlstd)
set_property(TARGET hermes-dpi PROPERTY POSITION_INDEPENDENT_CODE ON)