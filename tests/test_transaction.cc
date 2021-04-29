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
        auto transaction = std::make_shared<hermes::Transaction>(i);
        for (auto j = 0; j < num_events; j++) {
            auto e = std::make_shared<hermes::Event>(time++);
            transaction->add_event(e);
        }
        batch.emplace_back(std::move(transaction));
    }

    auto [record, schema] = batch.serialize();
    auto buffer = hermes::serialize(record, schema);
    EXPECT_TRUE(buffer);

    auto table = hermes::deserialize(buffer);

    auto new_batch_ptr = hermes::TransactionBatch::deserialize(table.get());
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

TEST(transaction_group, serilization) { // NOLINT
    hermes::TransactionGroupBatch batch;
    constexpr auto num_group = 1000;
    batch.reserve(num_group);
    uint64_t time = 0;
    for (auto i = 0; i < num_group; i++) {
        auto group = std::make_shared<hermes::TransactionGroup>();
        if (i % 2 == 0) {
            for (int j = 0; j < (i % 10 + 1); j++) {
                auto num_events = i % 10 + 1;
                auto transaction = std::make_shared<hermes::Transaction>();
                for (auto k = 0; k < num_events; k++) {
                    auto e = std::make_shared<hermes::Event>(time++);
                    transaction->add_event(e);
                }
                group->add_transaction(transaction);
            }
        } else {
            // two groups
            auto group1 = std::make_shared<hermes::TransactionGroup>();
            auto group2 = std::make_shared<hermes::TransactionGroup>();
            for (int j = 0; j < 10; j++) {
                auto transaction = std::make_shared<hermes::Transaction>(i);
                for (int k = 0; k < 5; k++) {
                    auto e = std::make_shared<hermes::Event>(time++);
                    transaction->add_event(e);
                }
                group1->add_transaction(transaction);
            }

            for (int j = 0; j < 10; j++) {
                auto transaction = std::make_shared<hermes::Transaction>(i);
                for (int k = 0; k < 5; k++) {
                    auto e = std::make_shared<hermes::Event>(time++);
                    transaction->add_event(e);
                }
                group2->add_transaction(transaction);
            }

            group->add_transaction(group1);
            group->add_transaction(group2);
        }
        batch.emplace_back(group);
    }

    auto [record, schema] = batch.serialize();
    auto buffer = hermes::serialize(record, schema);
    EXPECT_TRUE(buffer);

    auto table = hermes::deserialize(buffer);
    auto new_batch_ptr = hermes::TransactionGroupBatch::deserialize(table.get());
    auto &new_batch = *new_batch_ptr;
    EXPECT_EQ(new_batch.size(), num_group);
    auto group1 = new_batch[42];
    // should all be transactions
    for (auto const &m: group1->transaction_masks()) {
        EXPECT_FALSE(m);
    }
    auto group2 = new_batch[43];
    EXPECT_EQ(group2->transaction_masks().size(), 2);
    for (auto const &m: group2->transaction_masks()) {
        EXPECT_TRUE(m);
    }
}