set(PYBIND11_CPP_STANDARD -std=c++17)

pybind11_add_module(_pyhermes _pyhermes.cc)
target_link_libraries(_pyhermes PRIVATE hermes)