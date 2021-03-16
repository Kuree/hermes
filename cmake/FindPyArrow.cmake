# we assume pyarrow is installed
execute_process(COMMAND python3 -c
        "import pyarrow; print(pyarrow.get_include())"
        OUTPUT_VARIABLE PYARROW_INCLUDE_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process(COMMAND python3 -c
        "import pyarrow; print(pyarrow.get_library_dirs()[0])"
        OUTPUT_VARIABLE PYARROW_LIBS_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE)

find_library(LIBARROW_LIBRARY NAMES libarrow arrow libarrow.so.300
        HINTS ${PYARROW_LIBS_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_Args(PyArrow DEFAULT_MSG
                                  LIBARROW_LIBRARY PYARROW_INCLUDE_DIR)

if (LIBARROW_LIBRARY)
    add_library(arrow::arrow INTERFACE IMPORTED)
    set_property(TARGET arrow::arrow PROPERTY INTERFACE_LINK_LIBRARIES ${LIBARROW_LIBRARY})
endif()

mark_as_advanced(PYARROW_INCLUDE_DIR LIBARROW_LIBRARY)

# other arrow components
find_library(LIBPARQUET_LIBRARY NAMES libparquet parquet libparquet.so.300
        HINTS ${PYARROW_LIBS_DIR})


if (LIBPARQUET_LIBRARY)
    add_library(arrow::parquet INTERFACE IMPORTED)
    set_property(TARGET arrow::parquet PROPERTY INTERFACE_LINK_LIBRARIES ${LIBPARQUET_LIBRARY})
endif()

find_library(LIBPLASMA_LIBRARY NAMES libplasma plasma libplasma.so.300
        HINTS ${PYARROW_LIBS_DIR})

if (LIBPLASMA_LIBRARY)
    add_library(arrow::plasma INTERFACE IMPORTED)
    set_property(TARGET arrow::plasma PROPERTY INTERFACE_LINK_LIBRARIES ${LIBPLASMA_LIBRARY})
endif()
