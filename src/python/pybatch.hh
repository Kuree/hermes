#pragma once

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

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
    batch_class.def_property("name", &T::name, &T::set_name);

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

struct visitor {
    using ret = py::object;
    template <typename T>
    ret operator()(const T &value) {
        return py::cast(value);
    }
};

template <typename T>
void init_values(py::class_<T, std::shared_ptr<T>> &obj, const char *add_value_name) {
    obj.def(add_value_name, &T::template add_value<bool>, py::arg("name"), py::arg("value"));
    obj.def(add_value_name, &T::template add_value<uint64_t>, py::arg("name"), py::arg("value"));
    obj.def(add_value_name, &T::template add_value<std::string>, py::arg("name"), py::arg("value"));

    obj.def("__getattr__", [](const T &t, const std::string &name) -> py::object {
        auto const &values = t.values();
        if (values.find(name) == values.end()) {
            throw py::value_error("Event object does not have attribute " + name);
        } else {
            auto const &v = values.at(name);
            return py::detail::visit_helper<std::variant>::call(visitor(), v);
        }
    });
    // through get item
    obj.def("__getitem__", [](const T &e, const std::string &name) {
        auto obj = py::cast(e);
        auto name_obj = py::cast(name);
        return obj.attr(name_obj);
    });

    // set attribute as well
    obj.def("__setattr__",
            [](T &t, const std::string &name, bool value) { t.add_value(name, value); });
    obj.def("__setattr__",
            [](T &t, const std::string &name, uint64_t value) { t.add_value(name, value); });
    obj.def("__setattr__", [](T &t, const std::string &name, const std::string &value) {
        t.add_value(name, value);
    });

    // proxy through set item
    obj.def("__setitem__", [](T &t, const std::string &name, const py::object &value) {
        auto t_obj = py::cast(t);
        py::setattr(t_obj, name.c_str(), value);
    });
}