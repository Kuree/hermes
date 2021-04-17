add_subdirectory(fmt)
add_subdirectory(arrow)
add_subdirectory(pybind11)

# no need to add tests
set(SLANG_INCLUDE_TESTS OFF CACHE BOOL "Include test targets in the build" FORCE)
add_subdirectory(slang)
