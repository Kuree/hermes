function(add_tools_target target)
    add_executable(${target} ${target}.cc)
    target_link_libraries(${target} hermes)
endfunction()

add_tools_target(log2csv)
