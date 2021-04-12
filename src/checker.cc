#include "checker.hh"

#include <iostream>
#include <mutex>
#include <thread>

#include "fmt/format.h"

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
        std::optional<std::exception_ptr> assert;
        std::mutex assert_mutex;
        for (auto const &table : tables) {
            std::vector<std::shared_ptr<arrow::Table>> ts = {table.table};
            auto thread = std::thread([ts, loader, query, &assert_mutex, &assert, this]() {
                auto stream = TransactionStream(ts, loader.get());
                if (assert_exception_) {
                    try {
                        for (auto &&it : stream) {
                            {
                                std::lock_guard guard(assert_mutex);
                                if (assert) {
                                    break;
                                }
                            }
                            check(it.transaction, query);
                        }
                    } catch (const CheckerAssertion &ex) {
                        std::lock_guard guard(assert_mutex);
                        assert = std::current_exception();
                    }
                } else {
                    for (auto &&it : stream) {
                        check(it.transaction, query);
                    }
                }
            });
            threads.emplace_back(std::move(thread));
        }
        for (auto &thread : threads) {
            thread.join();
        }
        if (assert_exception_ && assert) {
            std::rethrow_exception(*assert);
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

void Checker::assert_(bool value, const std::string &message) const {
    if (!value) {
        if (assert_exception_) {
            throw CheckerAssertion(message);
        } else {
            if (message.empty()) {
                std::cerr << "[ASSERTION ERROR]:\n";
            } else {
                std::cerr << fmt::format("[ASSERTION ERROR]: {0}\n", message);
            }
        }
    }
}

}  // namespace hermes