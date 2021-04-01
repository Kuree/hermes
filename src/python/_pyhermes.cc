#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../logger.hh"
#include "../serializer.hh"
#include "../tracker.hh"

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
    event.def(py::init<uint64_t>(), py::arg("time"));
    event.def("add_value", &hermes::Event::add_value<bool>, py::arg("name"), py::arg("value"));
    event.def("add_value", &hermes::Event::add_value<uint8_t>, py::arg("name"), py::arg("value"));
    event.def("add_value", &hermes::Event::add_value<uint16_t>, py::arg("name"), py::arg("value"));
    event.def("add_value", &hermes::Event::add_value<uint32_t>, py::arg("name"), py::arg("value"));
    event.def("add_value", &hermes::Event::add_value<uint64_t>, py::arg("name"), py::arg("value"));
    event.def("add_value", &hermes::Event::add_value<std::string>, py::arg("name"),
              py::arg("value"));
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
    transaction.def(
        "add_event",
        py::overload_cast<const std::shared_ptr<hermes::Event> &>(&hermes::Transaction::add_event),
        py::arg("event"));
    transaction.def_property_readonly("id", &hermes::Transaction::id);
    transaction.def("finish", &hermes::Transaction::finish);
    transaction.def_property_readonly("finished", &hermes::Transaction::finished);
}

void init_serializer(py::module &m) {
    auto serializer = py::class_<hermes::Serializer>(m, "Serializer");
    serializer.def(py::init<const std::string &>());
    serializer.def("finalize", &hermes::Serializer::finalize);
}

void init_logger(py::module &m) {
    auto logger = py::class_<hermes::Logger>(m, "Logger");
    logger.def(py::init<std::string>());
    logger.def("log", &hermes::Logger::log, py::arg("event"));

    auto dummy_log_serializer =
        py::class_<hermes::DummyEventSerializer, std::shared_ptr<hermes::DummyEventSerializer>>(
            m, "DummyEventSerializer");
    dummy_log_serializer.def(py::init<>());
    dummy_log_serializer.def(py::init<std::string>(), py::arg("topic"));
    dummy_log_serializer.def(
        "connect", py::overload_cast<hermes::Serializer *>(&hermes::DummyEventSerializer::connect),
        py::arg("serializer"));
}

void init_tracker(py::module &m) {
    class PyTracker : public hermes::Tracker {
    public:
        using hermes::Tracker::Tracker;

        hermes::Transaction *track(hermes::Event *event) override {
            PYBIND11_OVERLOAD(hermes::Transaction *, hermes::Tracker, track, event);
        }
    };

    auto tracker =
        py::class_<hermes::Tracker, PyTracker, std::shared_ptr<hermes::Tracker>>(m, "Tracker");
    tracker.def("get_new_transaction", &hermes::Tracker::get_new_transaction,
                py::return_value_policy::copy);
    tracker.def("set_serializer", &hermes::Tracker::set_serializer, py::arg("serializer"));
    tracker.def("set_event_name", &hermes::Tracker::set_transaction_name);
    tracker.def("connect", &hermes::Tracker::connect);
    tracker.def("connect", [](hermes::Tracker &tracker, hermes::Serializer *serializer) {
        tracker.set_serializer(serializer);
        tracker.connect();
    });
}

void init_message_bus(py::module &m) {
    auto bus = py::class_<hermes::MessageBus>(m, "MessageBus");
    bus.def("flush", &hermes::MessageBus::stop);
    m.def("default_bus", &hermes::MessageBus::default_bus, py::return_value_policy::reference);
}

PYBIND11_MODULE(_pyhermes, m) {
    init_event(m);
    init_transaction(m);
    init_serializer(m);
    init_logger(m);
    init_tracker(m);
    init_message_bus(m);
}