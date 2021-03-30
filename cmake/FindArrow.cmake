set(ARROW_LIBS_DIR "${CMAKE_BINARY_DIR}/arrow/install/lib")
set(ARROW_INCLUDE_DIR "${CMAKE_BINARY_DIR}/arrow/install/include")
set(SNAPPY_LIB_DIR "${CMAKE_BINARY_DIR}/arrow/build/snappy_ep/src/snappy_ep-install/lib/")
set(THRIFT_LIB_DIR "${CMAKE_BINARY_DIR}/arrow/build/thrift_ep-install/lib/")

find_library(LIBARROW_LIBRARY NAMES arrow
        HINTS ${ARROW_LIBS_DIR})
find_library(LIBPARQUET_LIBRARY NAMES parquet
        HINTS ${ARROW_LIBS_DIR})
find_library(LIBSNAPPY_LIBRARY NAMES snappy
        HINTS ${SNAPPY_LIB_DIR})
find_library(LIBTHRIFT_LIBRARY NAMES thrift
        HINTS ${THRIFT_LIB_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_Args(Arrow DEFAULT_MSG
        LIBARROW_LIBRARY ARROW_INCLUDE_DIR)

mark_as_advanced(ARROW_INCLUDE_DIR LIBARROW_LIBRARY)

add_library(arrow::arrow INTERFACE IMPORTED)
set_property(TARGET arrow::arrow PROPERTY INTERFACE_LINK_LIBRARIES ${LIBARROW_LIBRARY})

add_library(arrow::parquet INTERFACE IMPORTED)
set_property(TARGET arrow::parquet PROPERTY INTERFACE_LINK_LIBRARIES ${LIBPARQUET_LIBRARY})

add_library(arrow::snappy INTERFACE IMPORTED)
set_property(TARGET arrow::snappy PROPERTY INTERFACE_LINK_LIBRARIES ${LIBSNAPPY_LIBRARY})

add_library(arrow::thrift INTERFACE IMPORTED)
set_property(TARGET arrow::thrift PROPERTY INTERFACE_LINK_LIBRARIES ${LIBTHRIFT_LIBRARY})
