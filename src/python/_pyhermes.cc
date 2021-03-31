#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../serializer.hh"

namespace py = pybind11;

struct visitor {
    using ret = py::object;
    template <typename T>
    ret operator()(const T &value) {
        return py::cast(value);
    }
};

void init_event(py::module &m) {
    auto event = py::class_<hermes::Event, std::shared_ptr<hermes::Event>>(m, "Event");
    event.def("add_value", &hermes::Event::add_value<bool>);
    event.def("add_value", &hermes::Event::add_value<uint8_t>);
    event.def("add_value", &hermes::Event::add_value<uint16_t>);
    event.def("add_value", &hermes::Event::add_value<uint32_t>);
    event.def("add_value", &hermes::Event::add_value<uint64_t>);
    event.def("add_value", &hermes::Event::add_value<std::string>);
    event.def_property_readonly("id", &hermes::Event::id);

    event.def("__getattr__", [](const hermes::Event &event, const std::string &name) -> py::object {
        auto const &values = event.values();
        if (values.find(name) == values.end()) {
            return py::none();
        } else {
            auto const &v = values.at(name);
            return py::detail::visit_helper<std::variant>::call(visitor(), v);
        }
    });

    event.def("__repr__", [](const hermes::Event &event) {
        auto const &values = event.values();
        auto result = py::dict();
        for (auto const &[name, v] : values) {
            auto value = py::detail::visit_helper<std::variant>::call(visitor(), v);
            result[name.c_str()] = value;
        }
        return py::str(result);
    });
}

void init_transaction(py::module &m) {
    auto transaction =
        py::class_<hermes::Transaction, std::shared_ptr<hermes::Transaction>>(m, "Transaction");
    transaction.def("add_event", py::overload_cast<const std::shared_ptr<hermes::Event> &>(
                                     &hermes::Transaction::add_event));
    transaction.def_property_readonly("id", &hermes::Transaction::id);
    transaction.def("finish", &hermes::Transaction::finish);
    transaction.def_property_readonly("finished", &hermes::Transaction::finished);
}

PYBIND11_MODULE(_pyhermes, m) {
    init_event(m);
    init_transaction(m);
}