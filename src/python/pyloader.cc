#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../json.hh"
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
            return hermes::json::serialize(v, pretty_print);
        },
        py::arg("pretty_print") = false);

    stream.def("where", [](const hermes::TransactionStream &stream,
                           const std::function<bool(const hermes::TransactionData &)> &filter) {
        pybind11::gil_scoped_release release;
        auto res = stream.where(filter);
        pybind11::gil_scoped_acquire acquire;
        return res;
    });
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
            return hermes::json::serialize(v, pretty_print);
        },
        py::arg("pretty_print") = false);

    data.def_property_readonly("start_time", [](const hermes::TransactionData &d) {
        if (d.is_group()) {
            return (*d.group).group->start_time();
        } else {
            return d.transaction->start_time();
        }
    });

    data.def_property_readonly("end_time", [](const hermes::TransactionData &d) {
        if (d.is_group()) {
            return (*d.group).group->end_time();
        } else {
            return d.transaction->end_time();
        }
    });
}
void init_fs_info(py::module &m) {
    auto fs = py::class_<hermes::FileSystemInfo>(m, "FileSystemInfo");
    fs.def(py::init<const std::string &>(), py::arg("path"));
    fs.def_property_readonly("is_s3", &hermes::FileSystemInfo::is_s3);
    fs.def_readwrite("path", &hermes::FileSystemInfo::path);
    fs.def_readwrite("access_key", &hermes::FileSystemInfo::access_key);
    fs.def_readwrite("secret_key", &hermes::FileSystemInfo::secret_key);
    fs.def_readwrite("endpoint", &hermes::FileSystemInfo::end_point);

    fs.def("clear", &hermes::FileSystemInfo::clear);
}

void init_enum(py::module &m) {
    auto type = py::enum_<hermes::EventDataType>(m, "EventDataType");
    type.value("bool", hermes::EventDataType::bool_);
    type.value("uint8", hermes::EventDataType::uint8_t_);
    type.value("uint16", hermes::EventDataType::uint16_t_);
    type.value("uint32", hermes::EventDataType::uint32_t_);
    type.value("uint64", hermes::EventDataType::uint64_t_);
    type.value("string", hermes::EventDataType::string);
}

void init_loader(py::module &m) {
    // init fs info first for the constructor
    init_fs_info(m);

    auto loader = py::class_<hermes::Loader, std::shared_ptr<hermes::Loader>>(m, "Loader");
    loader.def(py::init<std::string>(), py::arg("data_dir"));
    loader.def(py::init<std::vector<std::string>>(), py::arg("data_dirs"));
    loader.def(py::init<const std::vector<hermes::FileSystemInfo> &>(), py::arg("file_infos"));
    loader.def(py::init([](hermes::FileSystemInfo info) {
        std::vector<hermes::FileSystemInfo> infos = {std::move(info)};
        return std::make_unique<hermes::Loader>(infos);
    }));
    loader.def(py::init([](const py::args &args) {
        std::vector<std::string> dirs;
        dirs.reserve(args.size());
        for (auto const &dir : args) {
            auto str = dir.cast<std::string>();
            dirs.emplace_back(str);
        }
        return std::make_unique<hermes::Loader>(dirs);
    }));

    loader.def("__getitem__", [](hermes::Loader &loader, const std::string &name) {
        auto stream = loader.get_transaction_stream(name);
        return stream;
    });
    loader.def("__getitem__",
               [](hermes::Loader &loader,
                  const std::pair<std::string, std::pair<uint64_t, uint64_t>> &values) {
                   auto const &[name, time] = values;
                   auto [start, end] = time;
                   auto stream = loader.get_transaction_stream(name, start, end);
                   return stream;
               });
    loader.def("stream", [](hermes::Loader &loader) { loader.stream(); });
    loader.def("stream", py::overload_cast<bool>(&hermes::Loader::stream));

    loader.def_property_readonly("event_names", &hermes::Loader::get_event_names);
    loader.def_property_readonly("transaction_names", &hermes::Loader::get_transaction_names);
    loader.def_property_readonly("transaction_group_names",
                                 &hermes::Loader::get_transaction_group_names);

    loader.def("get_event_schema", &hermes::Loader::get_event_schema, py::arg("event_name"));
    loader.def("print_files", &hermes::Loader::print_files);

    init_stream(m);
    init_data(m);
    init_enum(m);
}