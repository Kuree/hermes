#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../json.hh"
#include "../loader.hh"
#include "../serializer.hh"

namespace py = pybind11;

void init_stream(py::module &m) {
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

    stream.def(
        "json",
        [](const std::shared_ptr<hermes::TransactionStream> &s, bool pretty_print) {
            rapidjson::MemoryPoolAllocator<> allocator;
            auto v = hermes::json::serialize(allocator, s);
            return hermes::json::serialize(v, true);
        },
        py::arg("pretty_print") = true);
}

uint64_t get_size(const hermes::TransactionData &t) {
    uint64_t size;
    if (t.is_group()) {
        size = (*t.group).values.size();
    } else {
        size = t.events->size();
    }
    return size;
}

void init_data(py::module &m) {
    // we adjust the interface a little bit differently from C++ version.
    // here we actually hide the transaction
    auto data = py::class_<hermes::TransactionData, std::shared_ptr<hermes::TransactionData>>(
        m, "TransactionData");

    data.def(
        "__getitem__",
        [](const hermes::TransactionData &t, int64_t index) {
            // depends on whether it carries group data or not
            uint64_t size = get_size(t);

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
    data.def("__len__", [](const hermes::TransactionData &t) { return get_size(t); });
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
    data.def_property_readonly("name", [](const hermes::TransactionData &t) {
        if (t.is_group()) {
            return (*t.group).group->name();
        } else {
            return t.transaction->name();
        }
    });

    data.def(
        "json",
        [](const hermes::TransactionData &d, bool pretty_print) {
            rapidjson::MemoryPoolAllocator<> allocator;
            auto v = hermes::json::serialize(allocator, d);
            return hermes::json::serialize(v, true);
        },
        py::arg("pretty_print") = true);
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

    init_stream(m);
    init_data(m);
}