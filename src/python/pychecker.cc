#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../checker.hh"

namespace py = pybind11;

void init_checker(py::module &m) {
    class PyChecker : public hermes::Checker {
        using hermes::Checker::Checker;

        void check(const hermes::TransactionData &data,
                   const std::shared_ptr<hermes::QueryHelper> &query) override {
            /* Acquire GIL before calling Python code */
            py::gil_scoped_acquire acquire;
            PYBIND11_OVERRIDE_PURE(void, hermes::Checker, check, data, query);
        }
    };

    auto checker =
        py::class_<hermes::Checker, PyChecker, std::shared_ptr<hermes::Checker>>(m, "Checker");
    checker.def(py::init<>());
    checker.def("check", &hermes::Checker::check, py::arg("transaction"), py::arg("query_helper"));
    checker.def(
        "run",
        [](hermes::Checker &checker, const std::string &transaction_name,
           const std::shared_ptr<hermes::Loader> &loader) {
            /* Release GIL before calling into (potentially long-running) C++ code */
            py::gil_scoped_release release;
            checker.run(transaction_name, loader);
        },
        py::arg("transaction_name"), py::arg("loader"));
    checker.def(
        "assert_",
        [](const hermes::Checker &checker, bool condition) { checker.assert_(condition); },
        py::arg("condition"));
    checker.def(
        "assert_",
        [](const hermes::Checker &checker, bool condition, const std::string &message) {
            checker.assert_(condition, message);
        },
        py::arg("condition"), py::arg("message"));
    checker.def_property("assert_exception", &hermes::Checker::assert_exception,
                         &hermes::Checker::set_assert_exception);
    checker.def_property("stateless", &hermes::Checker::stateless, &hermes::Checker::set_stateless);

    py::register_exception<hermes::CheckerAssertion>(m, "CheckerAssertion");
}
