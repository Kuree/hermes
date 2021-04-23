#include "../event.hh"
#include "../json.hh"
#include "../pubsub.hh"
#include "pybatch.hh"

namespace py = pybind11;

struct visitor {
    using ret = py::object;
    template <typename T>
    ret operator()(const T &value) {
        return py::cast(value);
    }
};

void init_event_util(py::module &m) {
    m.def("parse_event_log_fmt",
          py::overload_cast<const std::string &, const std::string &, const std::string &,
                            const std::vector<std::string> &>(&hermes::parse_event_log_fmt));
    m.def("parse_event_log_fmt",
          py::overload_cast<const std::string &, const std::string &, const std::string &,
                            const std::vector<std::string> &, hermes::MessageBus *>(
              &hermes::parse_event_log_fmt));
}

void init_event(py::module &m) {
    auto event = py::class_<hermes::Event, std::shared_ptr<hermes::Event>>(m, "Event");
    event.def(py::init<uint64_t>(), py::arg("time"));
    event.def("add_value", &hermes::Event::add_value<bool>, py::arg("name"), py::arg("value"));
    event.def("add_value", &hermes::Event::add_value<uint64_t>, py::arg("name"), py::arg("value"));
    event.def("add_value", &hermes::Event::add_value<std::string>, py::arg("name"),
              py::arg("value"));
    event.def_property_readonly("id", &hermes::Event::id);

    event.def("__getattr__", [](const hermes::Event &event, const std::string &name) -> py::object {
        auto const &values = event.values();
        if (values.find(name) == values.end()) {
            throw py::value_error("Event object does not have attribute " + name);
        } else {
            auto const &v = values.at(name);
            return py::detail::visit_helper<std::variant>::call(visitor(), v);
        }
    });
    // through get item
    event.def("__getitem__", [](const hermes::Event &e, const std::string &name) {
        auto obj = py::cast(e);
        auto name_obj = py::cast(name);
        return obj.attr(name_obj);
    });

    // set attribute as well
    event.def("__setattr__", [](hermes::Event &event, const std::string &name, bool value) {
        event.add_value(name, value);
    });
    event.def("__setattr__", [](hermes::Event &event, const std::string &name, uint64_t value) {
        event.add_value(name, value);
    });
    event.def("__setattr__", [](hermes::Event &event, const std::string &name,
                                const std::string &value) { event.add_value(name, value); });

    event.def(
        "json",
        [](const std::shared_ptr<hermes::Event> &event, bool pretty_print) -> std::string {
            rapidjson::MemoryPoolAllocator<> allocator;
            auto v = hermes::json::serialize(allocator, event);
            return hermes::json::serialize(v, pretty_print);
        },
        py::arg("pretty_print") = false);

    // proxy through set item
    event.def("__setitem__",
              [](hermes::Event &event, const std::string &name, const py::object &value) {
                  auto event_obj = py::cast(event);
                  py::setattr(event_obj, name.c_str(), value);
              });

    event.def("__repr__", [](const hermes::Event &event) {
        // repr is expensive so it's for debugging only
        auto const &values = event.values();
        auto result = py::dict();
        for (auto const &[name, v] : values) {
            auto value = py::detail::visit_helper<std::variant>::call(visitor(), v);
            result[name.c_str()] = value;
        }
        return py::str(result);
    });

    auto event_batch =
        py::class_<hermes::EventBatch, std::shared_ptr<hermes::EventBatch>>(m, "EventBatch");
    init_batch(event_batch);

    event_batch.def(
        "json",
        [](const std::shared_ptr<hermes::EventBatch> &events, bool pretty_print) -> std::string {
            rapidjson::MemoryPoolAllocator<> allocator;
            auto v = hermes::json::serialize(allocator, events);
            return hermes::json::serialize(v, pretty_print);
        },
        py::arg("pretty_print") = false);

    init_event_util(m);
}
