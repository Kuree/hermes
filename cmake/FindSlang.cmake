set(SLANG_LIBS_DIR "${CMAKE_BINARY_DIR}/slang/install/lib")
set(SLANG_INCLUDE_DIR "${CMAKE_BINARY_DIR}/slang/install/include")

find_library(LIBSLANG_LIBRARY NAMES slangcompiler
        HINTS ${SLANG_LIBS_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_Args(Slang DEFAULT_MSG
        LIBSLANG_LIBRARY SLANG_INCLUDE_DIR)

mark_as_advanced(SLANG_INCLUDE_DIR LIBSLANG_LIBRARY)

add_library(slang::compiler INTERFACE IMPORTED)
set_property(TARGET slang::compiler PROPERTY INTERFACE_LINK_LIBRARIES ${LIBSLANG_LIBRARY})