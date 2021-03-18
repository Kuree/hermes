include(setup_arrow.cmake)

add_library(arrow::arrow INTERFACE IMPORTED)
set_property(TARGET arrow::arrow PROPERTY INTERFACE_LINK_LIBRARIES ${ARROW_LIB_PATH})
add_library(arrow::parquet INTERFACE IMPORTED)
set_property(TARGET arrow::parquet PROPERTY INTERFACE_LINK_LIBRARIES ${PARQUET_LIB_PATH})
add_library(arrow::plasma INTERFACE IMPORTED)
set_property(TARGET arrow::plasma PROPERTY INTERFACE_LINK_LIBRARIES ${PLASMA_LIB_PATH})
