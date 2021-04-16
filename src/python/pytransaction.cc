#include "../transaction.hh"
#include "pybatch.hh"

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
    transaction.def_property("name", &hermes::Transaction::name, &hermes::Transaction::set_name);

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
    transaction_group.def_property("name", &hermes::TransactionGroup::name,
                                   &hermes::TransactionGroup::set_name);
    transaction_group.def("finish", &hermes::TransactionGroup::finish);
    transaction_group.def_property_readonly("finished", &hermes::TransactionGroup::finished);

    auto transaction_group_batch =
        py::class_<hermes::TransactionGroupBatch, std::shared_ptr<hermes::TransactionGroupBatch>>(
            m, "TransactionGroupBatch");
    init_batch(transaction_group_batch);
}
