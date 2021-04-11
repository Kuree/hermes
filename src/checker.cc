#include "checker.hh"

#include <thread>

namespace hermes {

void Checker::run(const std::string &transaction_name, const std::shared_ptr<Loader> &loader) {
    auto tables =
        loader->load_transaction_table(transaction_name, 0, std::numeric_limits<uint64_t>::max());
    auto query = std::make_shared<QueryHelper>(loader);
    // depends on whether it is stateless or not
    if (stateless_) {
        // run it in parallel. each thread gets a table
        std::vector<std::thread> threads;
        threads.reserve(tables.size());
        for (auto const &table : tables) {
            std::vector<std::shared_ptr<arrow::Table>> ts = {table.table};
            auto thread = std::thread([ts, loader, query, this]() {
                auto stream = TransactionStream(ts, loader.get());
                for (auto &&it : stream) {
                    check(it.transaction, query);
                }
            });
            threads.emplace_back(std::move(thread));
        }
        for (auto &thread : threads) {
            thread.join();
        }
    } else {
        // linear
        std::vector<std::shared_ptr<arrow::Table>> ts;
        ts.reserve(tables.size());
        for (auto const &res : tables) {
            ts.emplace_back(res.table);
        }
        auto stream = TransactionStream(ts, loader.get());
        for (auto &&it : stream) {
            check(it.transaction, query);
        }
    }
}

}  // namespace hermes