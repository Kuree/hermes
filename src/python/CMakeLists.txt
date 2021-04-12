set(PYBIND11_CPP_STANDARD -std=c++17)

pybind11_add_module(pyhermes pyhermes.cc)
target_link_libraries(pyhermes PRIVATE hermes)