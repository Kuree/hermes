#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../loader.hh"
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

    auto event_batch =
        py::class_<hermes::EventBatch, std::shared_ptr<hermes::EventBatch>>(m, "EventBatch");
    event_batch.def(
        "__getitem__", [](const hermes::EventBatch &batch, uint64_t index) { return batch[index]; },
        py::arg("index"));
    event_batch.def("__len__", [](const hermes::EventBatch &batch) { return batch.size(); });
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
    auto serializer =
        py::class_<hermes::Serializer, std::shared_ptr<hermes::Serializer>>(m, "Serializer");
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
    dummy_log_serializer.def("connect",
                             py::overload_cast<const std::shared_ptr<hermes::Serializer> &>(
                                 &hermes::DummyEventSerializer::connect),
                             py::arg("serializer"));
}

void init_loader(py::module &m) {
    auto loader = py::class_<hermes::Loader, std::shared_ptr<hermes::Loader>>(m, "Loader");
    loader.def(py::init<std::string>(), "data_dir");
    loader.def("__getitem__", [](hermes::Loader &loader, std::string &name) {
        auto stream = loader.get_transaction_stream(name);
        return stream;
    });

    auto stream = py::class_<hermes::TransactionStream, std::shared_ptr<hermes::TransactionStream>>(
        m, "TransactionStream");
    stream.def("__iter__", [](const hermes::TransactionStream &stream) {
        return py::make_iterator(stream.begin(), stream.end());
    });

    // we adjust the interface a little bit differently from C++ version.
    // here we actually hide the transaction
    auto iter =
        py::class_<hermes::TransactionDataIter, std::shared_ptr<hermes::TransactionDataIter>>(
            m, "TransactionData");

    iter.def(
        "__getitem__",
        [](const hermes::TransactionDataIter &iter, uint64_t index) {
            if (index >= (*iter).events->size()) {
                throw py::index_error();
            }
            return (*(*iter).events)[index];
        },
        py::arg("index"));
    iter.def("__iter__", [](const hermes::TransactionDataIter &iter) {
        return py::make_iterator((*iter).events->begin(), (*iter).events->end());
    });
    iter.def_property_readonly("finished", [](const hermes::TransactionDataIter &iter) {
        return iter.operator*().transaction->finished();
    });
}

void init_tracker(py::module &m) {
    class PyTracker : public hermes::Tracker {
    public:
        using hermes::Tracker::Tracker;

        hermes::Transaction *track(hermes::Event *event) override {
            PYBIND11_OVERRIDE_PURE(hermes::Transaction *, hermes::Tracker, track, event);
        }
    };

    auto tracker =
        py::class_<hermes::Tracker, PyTracker, std::shared_ptr<hermes::Tracker>>(m, "Tracker");
    tracker.def("get_new_transaction", &hermes::Tracker::get_new_transaction,
                py::return_value_policy::reference_internal);
    tracker.def("set_serializer", &hermes::Tracker::set_serializer, py::arg("serializer"));
    tracker.def("set_event_name", &hermes::Tracker::set_transaction_name);
    tracker.def("connect", &hermes::Tracker::connect);
    tracker.def(
        "connect",
        [](hermes::Tracker &tracker, const std::shared_ptr<hermes::Serializer> &serializer) {
            tracker.set_serializer(serializer);
            tracker.connect();
        },
        py::arg("serializer"));
    tracker.def(py::init<const std::string &>());
    tracker.def("track", &hermes::Tracker::track);
    tracker.def_property("transaction_name", &hermes::Tracker::transaction_name,
                         &hermes::Tracker::set_transaction_name);
}

void init_message_bus(py::module &m) {
    auto bus = py::class_<hermes::MessageBus, std::shared_ptr<hermes::MessageBus>>(m, "MessageBus");
    bus.def("flush", &hermes::MessageBus::stop);
    m.def(
        "default_bus",
        []() {
            auto *bus = hermes::MessageBus::default_bus();
            return bus->shared_from_this();
        },
        py::return_value_policy::reference_internal);
}

PYBIND11_MODULE(_pyhermes, m) {
    init_event(m);
    init_transaction(m);
    init_serializer(m);
    init_logger(m);
    init_tracker(m);
    init_message_bus(m);
    init_loader(m);
}