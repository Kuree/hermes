#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../checker.hh"
#include "../logger.hh"
#include "../rtl.hh"
#include "../serializer.hh"

namespace py = pybind11;

void init_tracker(py::module &m);
void init_event(py::module &m);
void init_transaction(py::module &m);
void init_logger(py::module &m);
void init_loader(py::module &m);
void init_checker(py::module &m);
void init_rtl(py::module &m);

void init_serializer(py::module &m) {
    auto serializer =
        py::class_<hermes::Serializer, std::shared_ptr<hermes::Serializer>>(m, "Serializer");
    serializer.def(py::init<const std::string &>(), py::arg("output_dir"));
    serializer.def(py::init<const std::string &, bool>(), py::arg("output_dir"),
                   py::arg("override"));
    serializer.def("finalize", &hermes::Serializer::finalize);
}

void init_query(py::module &m) {
    auto helper =
        py::class_<hermes::QueryHelper, std::shared_ptr<hermes::QueryHelper>>(m, "QueryHelper");
    helper.def(py::init<std::shared_ptr<hermes::Loader>>());
    helper.def("concurrent_events",
               py::overload_cast<uint64_t>(&hermes::QueryHelper::concurrent_events));
    helper.def("concurrent_events",
               py::overload_cast<uint64_t, uint64_t>(&hermes::QueryHelper::concurrent_events));
    helper.def("concurrent_events", py::overload_cast<const std::string &, uint64_t, uint64_t>(
                                        &hermes::QueryHelper::concurrent_events));
    helper.def("concurrent_events", py::overload_cast<const std::shared_ptr<hermes::Event> &>(
                                        &hermes::QueryHelper::concurrent_events));
    helper.def("concurrent_events",
               py::overload_cast<const std::string &, const std::shared_ptr<hermes::Event> &>(
                   &hermes::QueryHelper::concurrent_events));

    helper.def("concurrent_transactions",
               py::overload_cast<uint64_t>(&hermes::QueryHelper::concurrent_transactions));
    helper.def("concurrent_transactions", py::overload_cast<uint64_t, uint64_t>(
                                              &hermes::QueryHelper::concurrent_transactions));
    helper.def("concurrent_transactions",
               py::overload_cast<const std::string &, uint64_t, uint64_t>(
                   &hermes::QueryHelper::concurrent_transactions));
    helper.def("concurrent_transactions",
               py::overload_cast<const std::shared_ptr<hermes::Transaction> &>(
                   &hermes::QueryHelper::concurrent_transactions));
    helper.def("concurrent_transactions",
               py::overload_cast<const std::string &, const std::shared_ptr<hermes::Transaction> &>(
                   &hermes::QueryHelper::concurrent_transactions));
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

    auto sub = py::class_<hermes::Subscriber, std::shared_ptr<hermes::Subscriber>>(m, "Subscriber");
    sub.def_property("priority", &hermes::Subscriber::priority, &hermes::Subscriber::set_priority);
}

void init_meta(py::module &m) {
    // some metadata functions
    m.def("reset", []() {
        hermes::Event::reset_id();
        hermes::Transaction::reset_id();
        hermes::TransactionGroup::reset_id();
    });
}

PYBIND11_MODULE(pyhermes, m) {
    init_event(m);
    init_transaction(m);
    init_serializer(m);
    init_logger(m);
    init_message_bus(m);
    init_tracker(m);
    init_loader(m);
    init_query(m);
    init_checker(m);
    init_rtl(m);
    init_meta(m);
}