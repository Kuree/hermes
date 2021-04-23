function(add_link_include target)
    target_link_libraries(${target} gtest hermes gtest_main)
    target_include_directories(${target} PRIVATE ../src ../extern/googletest/googletest/include ${ARROW_INCLUDE_DIR})
endfunction()

function(add_link_include_static target)
    target_link_libraries(${target} hermes-dpi -static-libgcc -static-libstdc++)
    target_include_directories(${target} PRIVATE ../src ../extern/googletest/googletest/include ${ARROW_INCLUDE_DIR})
    # we need these to avoid linker hardcode the path of hermes-dpi
    set_target_properties(${target} PROPERTIES
            BUILD_WITH_INSTALL_RPATH TRUE
            INSTALL_RPATH_USE_LINK_PATH TRUE)
endfunction()

function(setup_test_target target)
    add_executable(${target} ${target}.cc)
    add_link_include(${target})
    gtest_discover_tests(${target})
endfunction()

setup_test_target(test_event)
setup_test_target(test_transaction)
setup_test_target(test_serialization)
setup_test_target(test_tracker)
setup_test_target(test_sv)
setup_test_target(test_loader)
setup_test_target(test_query)
setup_test_target(test_checker)
setup_test_target(test_rtl)
setup_test_target(test_pubsub)

# add as a library
add_library(test_tracker_lib SHARED test_tracker_lib.cc)
add_link_include_static(test_tracker_lib)

# only enabled when we're interested in performance test
if (PERFORMANCE_TEST)
    add_compile_definitions("PERFORMANCE_TEST")
endif()
