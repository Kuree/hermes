set(PYBIND11_CPP_STANDARD -std=c++17)

pybind11_add_module(pyhermes pyhermes.cc pytracker.cc pyevent.cc pytransaction.cc pylogger.cc pyloader.cc pychecker.cc
        pyrtl.cc)
target_link_libraries(pyhermes PRIVATE hermes)