#include "arrow.hh"
#include "arrow/api.h"
#include "gtest/gtest.h"
#include "transaction.hh"

TEST(transaction, serialization) {  // NOLINT
    hermes::TransactionBatch batch;
    constexpr auto num_transactions = 100;
    batch.reserve(num_transactions);
    uint64_t time = 0;
    for (auto i = 0; i < num_transactions; i++) {
        auto num_events = i % 10;
        auto transaction = std::make_unique<hermes::Transaction>(i);
        for (auto j = 0; j < num_events; j++) {
            auto e = std::make_unique<hermes::Event>(time++);
            transaction->add_event(e);
        }
        batch.emplace_back(std::move(transaction));
    }

    auto [record, schema] = batch.serialize();
    auto buffer = hermes::serialize(record, schema);
    EXPECT_TRUE(buffer);

    auto new_batch_ptr = hermes::TransactionBatch::deserialize(buffer);
    auto &new_batch = *new_batch_ptr;
    EXPECT_EQ(new_batch.size(), batch.size());
    auto const &ref_t = batch[42];
    auto const &test_t = new_batch[42];
    EXPECT_EQ(test_t->id(), ref_t->id());
    EXPECT_EQ(test_t->start_time(), ref_t->start_time());
    EXPECT_EQ(test_t->events().size(), ref_t->events().size());
    for (auto i = 0; i < test_t->events().size(); i++) {
        EXPECT_EQ(test_t->events()[i], ref_t->events()[i]);
    }
}