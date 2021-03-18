set(ARROW_LIBS_DIR "${CMAKE_BINARY_DIR}/arrow/install/lib")
set(ARROW_INCLUDE_DIR "${CMAKE_BINARY_DIR}/arrow/install/include")

find_library(LIBARROW_LIBRARY NAMES arrow
        HINTS ${ARROW_LIBS_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_Args(Arrow DEFAULT_MSG
        LIBARROW_LIBRARY ARROW_INCLUDE_DIR)

mark_as_advanced(ARROW_INCLUDE_DIR LIBARROW_LIBRARY)

add_library(arrow::arrow INTERFACE IMPORTED)
set_property(TARGET arrow::arrow PROPERTY INTERFACE_LINK_LIBRARIES ${LIBARROW_LIBRARY})

add_library(arrow::parquet INTERFACE IMPORTED)
set_property(TARGET arrow::parquet PROPERTY INTERFACE_LINK_LIBRARIES ${LIBPARQUET_LIBRARY})

add_library(arrow::plasma INTERFACE IMPORTED)
set_property(TARGET arrow::plasma PROPERTY INTERFACE_LINK_LIBRARIES ${LIBPLASMA_LIBRARY})
