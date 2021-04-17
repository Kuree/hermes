#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../rtl.hh"

namespace py = pybind11;

py::object access_rtl(const hermes::RTL &rtl, const std::string &name) {
    if (rtl.has_package(name)) {
        return py::cast(rtl.package(name));
    } else {
        return py::cast(rtl.get(name));
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

    // package proxy
    auto pkg = py::class_<hermes::PackageProxy>(m, "PackageProxy");
    pkg.def("__getitem__", &hermes::PackageProxy::get);
    pkg.def("__getattr__", &hermes::PackageProxy::get);
}
