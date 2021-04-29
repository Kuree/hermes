function(add_tools_target target)
    add_executable(${target} ${target}.cc)
    target_link_libraries(${target} hermes ${OPENSSL_SSL_LIBRARY})
endfunction()

add_tools_target(log2csv)
add_tools_target(server)
add_tools_target(log2spark)
target_include_directories(server PRIVATE ../../extern/cpp-httplib/)
