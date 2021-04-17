add_subdirectory(fmt)
add_subdirectory(arrow)
add_subdirectory(pybind11)

# no need to add tests
set(SLANG_INCLUDE_TESTS OFF CACHE BOOL "Include test targets in the build" FORCE)
add_subdirectory(slang)

# set some extra flags for external libs
set_property(TARGET slangcompiler PROPERTY POSITION_INDEPENDENT_CODE ON)
set_property(TARGET slangcore PROPERTY POSITION_INDEPENDENT_CODE ON)
set_property(TARGET slangparser PROPERTY POSITION_INDEPENDENT_CODE ON)
