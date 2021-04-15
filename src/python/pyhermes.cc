#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../checker.hh"
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

template <typename T>
void init_batch(py::class_<T, std::shared_ptr<T>> &batch_class) {
    batch_class.def(py::init<>());
    batch_class.def(
        "__getitem__",
        [](const T &batch, int64_t index) {
            if (index >= static_cast<int64_t>(batch.size())) throw py::index_error();
            if (index < -static_cast<int64_t>(batch.size())) throw py::index_error();
            if (index < 0) index += static_cast<int64_t>(batch.size());
            return batch[index];
        },
        py::arg("index"));
    batch_class.def("__len__", [](const T &batch) { return batch.size(); });
    batch_class.def("sort", &T::sort);
    batch_class.def("where", &T::where);
    batch_class.def("append", &T::emplace_back);

    // slice
    batch_class.def("__getitem__", [](const T &batch, const py::slice &slice) {
        py::ssize_t start, stop, step, slice_length;
        if (!slice.compute(static_cast<int64_t>(batch.size()), &start, &stop, &step,
                           &slice_length)) {
            throw py::error_already_set();
        }

        int i_start = static_cast<int>(start);
        int i_step = static_cast<int>(step);
        auto result = std::make_shared<T>();
        result->reserve(slice_length);
        for (int i = 0; i < slice_length; i++) {
            result->emplace_back(batch[i_start]);
            i_start += i_step;
        }
        return result;
    });
}

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
    event.def("__setattr__", [](hermes::Event &event, const std::string &name, uint8_t value) {
        event.add_value(name, value);
    });
    event.def("__setattr__", [](hermes::Event &event, const std::string &name, uint16_t value) {
        event.add_value(name, value);
    });
    event.def("__setattr__", [](hermes::Event &event, const std::string &name, uint32_t value) {
        event.add_value(name, value);
    });
    event.def("__setattr__", [](hermes::Event &event, const std::string &name, uint64_t value) {
        event.add_value(name, value);
    });
    event.def("__setattr__", [](hermes::Event &event, const std::string &name,
                                const std::string &value) { event.add_value(name, value); });

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
}

void init_transaction(py::module &m) {
    auto transaction =
        py::class_<hermes::Transaction, std::shared_ptr<hermes::Transaction>>(m, "Transaction");
    transaction.def(py::init<>());
    transaction.def(
        "add_event",
        py::overload_cast<const std::shared_ptr<hermes::Event> &>(&hermes::Transaction::add_event),
        py::arg("event"));
    transaction.def_property_readonly("id", &hermes::Transaction::id);
    transaction.def("finish", &hermes::Transaction::finish);
    transaction.def_property_readonly("finished", &hermes::Transaction::finished);

    auto transaction_batch =
        py::class_<hermes::TransactionBatch, std::shared_ptr<hermes::TransactionBatch>>(
            m, "TransactionBatch");
    init_batch(transaction_batch);

    // group
    auto transaction_group =
        py::class_<hermes::TransactionGroup, std::shared_ptr<hermes::TransactionGroup>>(
            m, "TransactionGroup");
    transaction_group.def(py::init<>());
    transaction_group.def("add_transaction",
                          py::overload_cast<const std::shared_ptr<hermes::TransactionGroup> &>(
                              &hermes::TransactionGroup::add_transaction));
    transaction_group.def("add_transaction",
                          py::overload_cast<const std::shared_ptr<hermes::Transaction> &>(
                              &hermes::TransactionGroup::add_transaction));
    transaction_group.def("__len__", &hermes::TransactionGroup::size);
    transaction_group.def_property_readonly("id", &hermes::TransactionGroup::id);

    auto transaction_group_batch =
        py::class_<hermes::TransactionGroupBatch, std::shared_ptr<hermes::TransactionGroupBatch>>(
            m, "TransactionGroupBatch");
    init_batch(transaction_group_batch);
}

void init_serializer(py::module &m) {
    auto serializer =
        py::class_<hermes::Serializer, std::shared_ptr<hermes::Serializer>>(m, "Serializer");
    serializer.def(py::init<const std::string &>(), py::arg("output_dir"));
    serializer.def(py::init<const std::string &, bool>(), py::arg("output_dir"),
                   py::arg("override"));
    serializer.def("finalize", &hermes::Serializer::finalize);
}

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
}

void init_loader(py::module &m) {
    auto loader = py::class_<hermes::Loader, std::shared_ptr<hermes::Loader>>(m, "Loader");
    loader.def(py::init<std::string>(), py::arg("data_dir"));
    loader.def(py::init<std::vector<std::string>>(), py::arg("data_dirs"));
    loader.def(py::init([](const py::args &args) {
        std::vector<std::string> dirs;
        dirs.reserve(args.size());
        for (auto const &dir : args) {
            auto str = dir.cast<std::string>();
            dirs.emplace_back(str);
        }
        return std::make_unique<hermes::Loader>(dirs);
    }));

    loader.def("__getitem__", [](hermes::Loader &loader, std::string &name) {
        auto stream = loader.get_transaction_stream(name);
        return stream;
    });
    loader.def("stream", [](hermes::Loader &loader) { loader.stream(); });
    loader.def("stream", py::overload_cast<bool>(&hermes::Loader::stream));

    auto stream = py::class_<hermes::TransactionStream, std::shared_ptr<hermes::TransactionStream>>(
        m, "TransactionStream");
    stream.def("__iter__", [](hermes::TransactionStream &stream) {
        return py::make_iterator(stream.begin(), stream.end());
    });
    stream.def("__len__", [](const hermes::TransactionStream &stream) { return stream.size(); });
    // we can also materialize transactions on demand
    stream.def(
        "__getitem__",
        [](hermes::TransactionStream &stream, uint64_t index) {
            if (index > stream.size()) {
                throw py::index_error();
            }
            auto pos = stream.begin() + index;
            return *pos;
        },
        py::arg("index"));

    // we adjust the interface a little bit differently from C++ version.
    // here we actually hide the transaction
    auto data = py::class_<hermes::TransactionData, std::shared_ptr<hermes::TransactionData>>(
        m, "TransactionData");

    data.def(
        "__getitem__",
        [](const hermes::TransactionData &t, int64_t index) {
            // depends on whether it carries group data or not
            uint64_t size;
            if (t.is_group()) {
                size = (*t.group).values.size();
            } else {
                size = t.events->size();
            }
            if (index >= static_cast<int64_t>(size) || index < -static_cast<int64_t>(size)) {
                throw py::index_error();
            }
            if (index < 0) {
                index += static_cast<int64_t>(size);
            }
            if (t.is_group()) {
                return py::cast((*t.group).values[index]);
            } else {
                return py::cast((*t.events)[index]);
            }
        },
        py::arg("index"));
    data.def("__iter__", [](const hermes::TransactionData &t) {
        if (t.is_group()) {
            return py::make_iterator((*t.group).values.begin(), (*t.group).values.end());
        } else {
            return py::make_iterator(t.events->begin(), t.events->end());
        }
    });
    data.def("__len__", [](const hermes::TransactionData &t) {
        uint64_t size;
        if (t.is_group()) {
            size = (*t.group).values.size();
        } else {
            size = t.events->size();
        }
        return size;
    });
    data.def_property_readonly("finished", [](const hermes::TransactionData &t) {
        // we assume the transaction group always deals with finished transaction
        if (t.is_group())
            return true;
        else
            return t.transaction->finished();
    });
    data.def_property_readonly("id", [](const hermes::TransactionData &t) {
        if (t.is_group()) {
            return (*t.group).group->id();
        } else {
            return t.transaction->id();
        }
    });
    data.def_property_readonly("is_group", &hermes::TransactionData::is_group);
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

void init_checker(py::module &m) {
    // we need to redirect some of the functions here
    class Checker_ : public hermes::Checker {
    public:
        void check(const std::shared_ptr<hermes::Transaction> &transaction,
                   const std::shared_ptr<hermes::QueryHelper> &query) override {
            auto loader = query->get_loader();
            auto events = loader->get_events(*transaction);
            hermes::TransactionData data{.transaction = transaction, .events = events};
            try {
                check(data, query);
            } catch (pybind11::error_already_set &e) {
                // a little hack to bypass pybind exception conversion
                std::lock_guard guard(assert_mutex_);
                auto ex = hermes::CheckerAssertion(std::string(e.what()));
                throw ex;
            }
        }

        virtual void check(const hermes::TransactionData &data,
                           const std::shared_ptr<hermes::QueryHelper> &query) = 0;
    };

    class PyChecker : public Checker_ {
        using Checker_::Checker_;

        void check(const hermes::TransactionData &data,
                   const std::shared_ptr<hermes::QueryHelper> &query) override {
            /* Acquire GIL before calling Python code */
            py::gil_scoped_acquire acquire;
            PYBIND11_OVERRIDE_PURE(void, Checker_, check, data, query);
        }
    };

    auto checker = py::class_<Checker_, PyChecker>(m, "Checker");
    checker.def(py::init<>());
    checker.def("check",
                py::overload_cast<const hermes::TransactionData &,
                                  const std::shared_ptr<hermes::QueryHelper> &>(&Checker_::check),
                py::arg("transaction"), py::arg("query_helper"));
    checker.def(
        "run",
        [](Checker_ &checker, const std::string &transaction_name,
           const std::shared_ptr<hermes::Loader> &loader) {
            /* Release GIL before calling into (potentially long-running) C++ code */
            py::gil_scoped_release release;
            checker.run(transaction_name, loader);
        },
        py::arg("transaction_name"), py::arg("loader"));
    checker.def(
        "assert_", [](const Checker_ &checker, bool condition) { checker.assert_(condition); },
        py::arg("condition"));
    checker.def(
        "assert_",
        [](const Checker_ &checker, bool condition, const std::string &message) {
            checker.assert_(condition, message);
        },
        py::arg("condition"), py::arg("message"));
    checker.def_property("assert_exception", &Checker_::assert_exception,
                         &Checker_::set_assert_exception);
    checker.def_property("stateless", &Checker_::stateless, &Checker_::set_stateless);

    py::register_exception<hermes::CheckerAssertion>(m, "CheckerAssertion");
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

PYBIND11_MODULE(pyhermes, m) {
    init_event(m);
    init_transaction(m);
    init_serializer(m);
    init_logger(m);
    init_tracker(m);
    init_message_bus(m);
    init_loader(m);
    init_query(m);
    init_checker(m);
}