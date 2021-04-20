set(ARROW_LIBS_DIR "${CMAKE_BINARY_DIR}/arrow/install/lib")
set(ARROW_INCLUDE_DIR "${CMAKE_BINARY_DIR}/arrow/install/include")
set(SNAPPY_LIB_DIR "${CMAKE_BINARY_DIR}/arrow/build/snappy_ep/src/snappy_ep-install/lib/")
set(THRIFT_LIB_DIR "${CMAKE_BINARY_DIR}/arrow/build/thrift_ep-install/lib/")
set(AWS_LIB_DIR "${CMAKE_BINARY_DIR}/arrow/build/awssdk_ep-install/lib/")

find_library(LIBARROW_LIBRARY NAMES arrow
        HINTS ${ARROW_LIBS_DIR})
find_library(LIBPARQUET_LIBRARY NAMES parquet
        HINTS ${ARROW_LIBS_DIR})
find_library(LIBSNAPPY_LIBRARY NAMES snappy
        HINTS ${SNAPPY_LIB_DIR})
find_library(LIBTHRIFT_LIBRARY NAMES thrift
        HINTS ${THRIFT_LIB_DIR})
find_library(LIBTHRIFT_LIBRARY NAMES thrift
        HINTS ${THRIFT_LIB_DIR})

# aws stuff
find_library(LIBAWS_S3_LIBRARY NAMES aws-cpp-sdk-s3
        HINTS ${AWS_LIB_DIR})
find_library(LIBAWS_CORE_LIBRARY NAMES aws-cpp-sdk-core
        HINTS ${AWS_LIB_DIR})
find_library(LIBAWS_CHECKSUM_LIBRARY NAMES aws-checksums
        HINTS ${AWS_LIB_DIR})
find_library(LIBAWS_C_COMMON_LIBRARY NAMES aws-c-common
        HINTS ${AWS_LIB_DIR})
find_library(LIBAWS_IAM_LIBRARY NAMES aws-cpp-sdk-identity-management
        HINTS ${AWS_LIB_DIR})
find_library(LIBAWS_STS_LIBRARY NAMES aws-cpp-sdk-sts
        HINTS ${AWS_LIB_DIR})
find_library(LIBAWS_C_COMMON_LIBRARY NAMES aws-c-common
        HINTS ${AWS_LIB_DIR})
find_library(LIBAWS_AUTH_LIBRARY NAMES aws-c-common
        HINTS ${AWS_LIB_DIR})

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

add_library(aws::s3 INTERFACE IMPORTED)
set_property(TARGET aws::s3 PROPERTY INTERFACE_LINK_LIBRARIES ${LIBAWS_S3_LIBRARY})
add_library(aws::aws INTERFACE IMPORTED)
set_property(TARGET aws::aws PROPERTY INTERFACE_LINK_LIBRARIES  ${LIBAWS_S3_LIBRARY} ${LIBAWS_CORE_LIBRARY} ${LIBAWS_C_EVENT_STREAM_LIBRARY}
        ${LIBAWS_C_COMMON_LIBRARY} ${LIBAWS_CHECKSUM_LIBRARY} ${LIBAWS_IAM_STREAM_LIBRARY} ${LIBAWS_STS_LIBRARY})
add_library(aws::core INTERFACE IMPORTED)
set_property(TARGET aws::core PROPERTY INTERFACE_LINK_LIBRARIES ${LIBAWS_CORE_LIBRARY})
add_library(aws::c INTERFACE IMPORTED)
set_property(TARGET aws::c PROPERTY INTERFACE_LINK_LIBRARIES ${LIBAWS_C_COMMON_LIBRARY})
add_library(aws::c-event INTERFACE IMPORTED)
set_property(TARGET aws::c-event PROPERTY INTERFACE_LINK_LIBRARIES ${LIBAWS_C_EVENT_STREAM_LIBRARY})
