#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../logger.hh"
#include "../serializer.hh"

namespace py = pybind11;

void init_logger(py::module &m) {
    auto logger = py::class_<hermes::Logger>(m, "Logger");
    logger.def(py::init<std::string>());
    logger.def("log",
               py::overload_cast<const std::shared_ptr<hermes::Event> &>(&hermes::Logger::log),
               py::arg("event"));
    logger.def(
        "log",
        py::overload_cast<const std::shared_ptr<hermes::Transaction> &>(&hermes::Logger::log),
        py::arg("transaction"));
    logger.def(
        "log",
        py::overload_cast<const std::shared_ptr<hermes::TransactionGroup> &>(&hermes::Logger::log),
        py::arg("group"));
    logger.def("log",
               py::overload_cast<const std::string &, const std::shared_ptr<hermes::Event> &>(
                   &hermes::Logger::log),
               py::arg("topic"), py::arg("event"));
    logger.def("log",
               py::overload_cast<const std::string &, const std::shared_ptr<hermes::Transaction> &>(
                   &hermes::Logger::log),
               py::arg("topic"), py::arg("transaction"));
    logger.def(
        "log",
        py::overload_cast<const std::string &, const std::shared_ptr<hermes::TransactionGroup> &>(
            &hermes::Logger::log),
        py::arg("topic"), py::arg("group"));

    auto dummy_log_serializer =
        py::class_<hermes::DummyEventSerializer, std::shared_ptr<hermes::DummyEventSerializer>>(
            m, "DummyEventSerializer");
    dummy_log_serializer.def(py::init<>());
    dummy_log_serializer.def(py::init<std::string>(), py::arg("topic"));
    dummy_log_serializer.def("connect",
                             py::overload_cast<const std::shared_ptr<hermes::Serializer> &>(
                                 &hermes::DummyEventSerializer::connect),
                             py::arg("serializer"));
    dummy_log_serializer.def("flush", &hermes::DummyEventSerializer::flush);

    m.def("event_in_order", &hermes::event_in_order);
    m.def("set_event_in_order", &hermes::set_event_in_order, py::arg("value"));
}
