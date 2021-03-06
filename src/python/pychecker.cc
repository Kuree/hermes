#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../checker.hh"

namespace py = pybind11;

class Checker_ : public hermes::Checker {
public:
    void check(const hermes::TransactionData &data,
               const std::shared_ptr<hermes::QueryHelper> &query) override {
        try {
            check(data, *query);
        } catch (pybind11::error_already_set &e) {
            // a little hack to bypass pybind exception conversion
            std::lock_guard guard(assert_mutex_);
            auto ex = hermes::CheckerAssertion(std::string(e.what()));
            throw ex;
        }
    }

    virtual void check(const hermes::TransactionData &data, hermes::QueryHelper &query) = 0;
};

class DummyChecker : public Checker_ {
    void check(const hermes::TransactionData &, hermes::QueryHelper &) override {
        // noop, just for performance measurement
    }
};

void init_checker(py::module &m) {
    class PyChecker : public Checker_ {
        using Checker_::Checker_;

        void check(const hermes::TransactionData &data, hermes::QueryHelper &query) override {
            /* Acquire GIL before calling Python code */
            py::gil_scoped_acquire acquire;
            PYBIND11_OVERRIDE_PURE(void, Checker_, check, data, query);
        }
    };

    auto checker = py::class_<Checker_, PyChecker, std::shared_ptr<Checker_>>(m, "Checker");
    checker.def(py::init<>());
    checker.def(
        "check",
        py::overload_cast<const hermes::TransactionData &, hermes::QueryHelper &>(&Checker_::check),
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
        "assert_", [](Checker_ &checker, bool condition) { checker.assert_(condition); },
        py::arg("condition"));
    checker.def(
        "assert_",
        [](Checker_ &checker, bool condition, const std::string &message) {
            checker.assert_(condition, message);
        },
        py::arg("condition"), py::arg("message"));
    checker.def_property("assert_exception", &Checker_::assert_exception,
                         &Checker_::set_assert_exception);
    checker.def_property("stateless", &Checker_::stateless, &Checker_::set_stateless);

    py::register_exception<hermes::CheckerAssertion>(m, "CheckerAssertion");

    // for performance measurement
    py::class_<DummyChecker, Checker_, std::shared_ptr<DummyChecker>>(m, "DummyChecker")
        .def(py::init<>());
}
