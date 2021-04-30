#include "../event.hh"
#include "../json.hh"
#include "../pubsub.hh"
#include "pybatch.hh"

namespace py = pybind11;

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

    event.def(
        "json",
        [](const std::shared_ptr<hermes::Event> &event, bool pretty_print) -> std::string {
            rapidjson::MemoryPoolAllocator<> allocator;
            auto v = hermes::json::serialize(allocator, event);
            return hermes::json::serialize(v, pretty_print);
        },
        py::arg("pretty_print") = false);

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

    init_values(event, "add_value");

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
