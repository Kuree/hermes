#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../tracker.hh"

namespace py = pybind11;

template <typename T, typename K>
void init_tracker_base(py::class_<T, K, hermes::Subscriber, std::shared_ptr<T>> &tracker) {
    tracker.def("get_new_transaction", &T::get_new_transaction,
                py::return_value_policy::reference_internal);
    tracker.def("set_serializer", &T::set_serializer, py::arg("serializer"));
    tracker.def_property("transaction_name", &T::transaction_name, &T::set_transaction_name);
    tracker.def("connect", &T::connect);
    tracker.def(
        "connect",
        [](T &tracker, const std::shared_ptr<hermes::Serializer> &serializer) {
            tracker.set_serializer(serializer);
            tracker.connect();
        },
        py::arg("serializer"));
    tracker.def(py::init<const std::string &>());
    tracker.def("track", &T::track);
    tracker.def_property("transaction_name", &T::transaction_name, &T::set_transaction_name);
    tracker.def_property("publish_transaction", &T::publish_transaction,
                         &T::set_publish_transaction);
}

class PyTracker : public hermes::Tracker {
public:
    using hermes::Tracker::Tracker;

    hermes::Transaction *track(hermes::Event *event) override {
        PYBIND11_OVERRIDE_PURE(hermes::Transaction *, hermes::Tracker, track, event);
    }
};

class PyGroupTracker : public hermes::GroupTracker {
public:
    using hermes::GroupTracker::GroupTracker;

    hermes::TransactionGroup *track(hermes::Transaction *transaction) override {
        PYBIND11_OVERRIDE_PURE(hermes::TransactionGroup *, hermes::GroupTracker, track,
                               transaction);
    }
};

void init_tracker(py::module &m) {
    auto tracker = py::class_<hermes::Tracker, PyTracker, hermes::Subscriber,
                              std::shared_ptr<hermes::Tracker>>(m, "Tracker");
    init_tracker_base(tracker);

    auto group_tracker = py::class_<hermes::GroupTracker, PyGroupTracker, hermes::Subscriber,
                                    std::shared_ptr<hermes::GroupTracker>>(m, "GroupTracker");
    init_tracker_base(group_tracker);
}
