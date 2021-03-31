add_subdirectory(fmt)
add_subdirectory(arrow)
add_subdirectory(pybind11)

# set some extra flags for external libs
set_property(TARGET fmt PROPERTY POSITION_INDEPENDENT_CODE ON)
