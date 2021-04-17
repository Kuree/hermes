set(SLANG_LIBS_DIR "${CMAKE_BINARY_DIR}/slang/install/lib")
set(SLANG_INCLUDE_DIR "${CMAKE_BINARY_DIR}/slang/install/include")

find_library(LIBSLANG_LIBRARY NAMES slangcompiler
        HINTS ${SLANG_LIBS_DIR})
find_library(LIBSLANG_PARSER_LIBRARY NAMES slangparser
        HINTS ${SLANG_LIBS_DIR})
find_library(LIBSLANG_CORE_LIBRARY NAMES slangcore
        HINTS ${SLANG_LIBS_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_Args(Slang DEFAULT_MSG
        LIBSLANG_LIBRARY SLANG_INCLUDE_DIR)

mark_as_advanced(SLANG_INCLUDE_DIR LIBSLANG_LIBRARY)

add_library(slang::compiler INTERFACE IMPORTED)
set_property(TARGET slang::compiler PROPERTY INTERFACE_LINK_LIBRARIES ${LIBSLANG_LIBRARY})

add_library(slang::parser INTERFACE IMPORTED)
set_property(TARGET slang::parser PROPERTY INTERFACE_LINK_LIBRARIES ${LIBSLANG_PARSER_LIBRARY})

add_library(slang::core INTERFACE IMPORTED)
set_property(TARGET slang::core PROPERTY INTERFACE_LINK_LIBRARIES ${LIBSLANG_CORE_LIBRARY})

MESSAGE(${LIBSLANG_CORE_LIBRARY})