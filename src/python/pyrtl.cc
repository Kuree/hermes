#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../rtl.hh"
#include "fmt/format.h"

namespace py = pybind11;

class Enum {
public:
    explicit Enum(std::unordered_map<std::string, uint64_t> values) : values(std::move(values)) {}
    std::optional<uint64_t> get(const std::string &name) {
        if (values.find(name) != values.end()) {
            return values.at(name);
        } else {
            return std::nullopt;
        }
    }

    std::optional<std::string> get(uint64_t value) {
        for (auto const &[name, v] : values) {
            if (v == value) {
                return name;
            }
        }
        return std::nullopt;
    }

    std::optional<std::string> flags(uint64_t value) {
        std::vector<std::string> names;
        for (auto const &[name, v] : values) {
            if ((v & value) == v) {
                names.emplace_back(name);
            }
        }
        if (names.empty()) {
            return std::nullopt;
        } else {
            // sort them since the values are unordered
            std::sort(names.begin(), names.end());
            return fmt::format("{0}", fmt::join(names.begin(), names.end(), " | "));
        }
    }

    [[nodiscard]] bool has_def(const std::string &name) {
        return values.find(name) != values.end();
    }
    std::unordered_map<std::string, uint64_t> values;
};

py::object access_rtl(const hermes::RTL &rtl, const std::string &name) {
    if (rtl.has_package(name)) {
        return py::cast(rtl.package(name));
    } else {
        // could be a access to enum
        auto const &pkg = rtl.package("");
        if (pkg->index.find(name) != pkg->index.end()) {
            return py::cast(Enum(pkg->index.at(name)));
        }
        return py::cast(rtl.get(name));
    }
}

py::object access_pkg(const hermes::PackageProxy &pkg, const std::string &name) {
    if (pkg.index.find(name) != pkg.index.end()) {
        return py::cast(Enum(pkg.index.at(name)));
    } else {
        return py::cast(pkg.get(name));
    }
}

void init_rtl(py::module &m) {
    auto rtl = py::class_<hermes::RTL>(m, "RTL");
    rtl.def(py::init<const std::string &>(), py::arg("filename"));
    rtl.def(py::init<const std::vector<std::string> &>(), py::arg("filenames"));
    rtl.def(py::init<const std::vector<std::string> &, const std::vector<std::string> &>(),
            py::arg("filenames"), py::arg("include_dirs"));
    rtl.def_property_readonly("has_error", &hermes::RTL::has_error);
    rtl.def_property_readonly("error_message", &hermes::RTL::error_message);

    rtl.def("__getitem__", &access_rtl);
    rtl.def("__getattr__", &access_rtl);

    rtl.def("lookup", py::overload_cast<uint64_t>(&hermes::RTL::lookup));
    rtl.def("lookup", py::overload_cast<uint64_t, const std::string &>(&hermes::RTL::lookup));
    rtl.def("lookup", py::overload_cast<uint64_t, const std::string &, const std::string &>(
                          &hermes::RTL::lookup));
    rtl.def(
        "enum",
        [](const hermes::RTL &rtl, const std::string &name) -> std::optional<Enum> {
            auto pkg = rtl.package("");
            if (pkg->index.find(name) == pkg->index.end()) {
                return std::nullopt;
            } else {
                return Enum(pkg->index.at(name));
            }
        },
        py::arg("enum_name"));

    // package proxy
    auto pkg =
        py::class_<hermes::PackageProxy, std::shared_ptr<hermes::PackageProxy>>(m, "PackageProxy");
    pkg.def("__getitem__", &access_pkg);
    pkg.def("__getattr__", &access_pkg);
    pkg.def(
        "enum",
        [](const hermes::PackageProxy &pkg, const std::string &name) -> std::optional<Enum> {
            if (pkg.index.find(name) == pkg.index.end()) {
                return std::nullopt;
            } else {
                return Enum(pkg.index.at(name));
            }
        },
        py::arg("enum_name"));

    // enum
    auto enum_ = py::class_<Enum>(m, "Enum");
    enum_.def("__getitem__", py::overload_cast<uint64_t>(&Enum::get));
    enum_.def("__getitem__", py::overload_cast<const std::string &>(&Enum::get));
    enum_.def("__getattr__", py::overload_cast<uint64_t>(&Enum::get));
    enum_.def("__getattr__", py::overload_cast<const std::string &>(&Enum::get));
    enum_.def("__len__", [](const Enum &e) { return e.values.size(); });
    enum_.def("__repr__", [](const Enum &e) {
        auto obj = py::cast(e.values);
        return py::str(obj);
    });
    enum_.def("flags", &Enum::flags, py::arg("value"));
}
