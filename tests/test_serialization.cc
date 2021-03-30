#include "arrow.hh"
#include "event.hh"
#include "gtest/gtest.h"
#include "loader.hh"
#include "serializer.hh"
#include "test_util.hh"


TEST(serialization, event) {  // NOLINT
    TempDirectory dir;

    hermes::EventBatch batch;
    constexpr auto num_event = 1000;
    for (auto i = 0; i < num_event; i++) {
        auto e = std::make_unique<hermes::Event>(i);
        e->add_value<uint64_t>("value1", i);
        batch.emplace_back(std::move(e));
    }

    EXPECT_TRUE(batch.validate());
    // serialize it
    hermes::Serializer s(dir.path());
    s.serialize(batch);
    s.finalize();

    hermes::Loader loader(dir.path());
    auto tables = loader.get_events(0, num_event);
    EXPECT_EQ(tables.size(), 1);
    auto table = tables[0];
    auto event_batch = hermes::EventBatch::deserialize(table);
    EXPECT_EQ(event_batch->size(), num_event);
    auto const &event = (*event_batch)[42];
    EXPECT_EQ(event->time(), 42);
    auto value = event->get_value<uint64_t>("value1");
    EXPECT_TRUE(value);
    EXPECT_EQ(*value, 42);
}

TEST(serialization, transactions) {  // NOLINT
    TempDirectory dir;

    hermes::TransactionBatch batch;
    constexpr auto num_transactions = 1000;
    for (auto i = 0; i < num_transactions; i++) {
        auto t = std::make_shared<hermes::Transaction>(i);
        for (uint32_t j = 0; j < i % 10; j++) {
            auto e = std::make_shared<hermes::Event>(i);
            e->set_time(i);
            t->add_event(e);
        }
        batch.emplace_back(t);
    }

    // serialize it
    hermes::Serializer s(dir.path());
    s.serialize(batch);
    s.finalize();

    hermes::Loader loader(dir.path());
    auto tables = loader.get_transactions(0, num_transactions);
    EXPECT_EQ(tables.size(), 1);
    auto table = tables[0];
    auto transaction_batch = hermes::TransactionBatch::deserialize(table);
    EXPECT_EQ(transaction_batch->size(), num_transactions);
    auto const &transaction = (*transaction_batch)[42];
    EXPECT_EQ(transaction->start_time(), 42);
    EXPECT_EQ(transaction->events().size(), 42 % 10);
}

TEST(serialization, get_events) {  // NOLINT
    TempDirectory dir;

    hermes::TransactionBatch transaction_batch;
    hermes::EventBatch event_batch;
    constexpr auto num_transactions = 1000;
    for (auto i = 0; i < num_transactions; i++) {
        auto t = std::make_shared<hermes::Transaction>(i);
        for (uint32_t j = 0; j < i % 10; j++) {
            auto e = std::make_shared<hermes::Event>(i);
            e->add_value<uint64_t>("value", i);
            e->set_time(i);
            t->add_event(e);
            event_batch.emplace_back(e);
        }
        transaction_batch.emplace_back(t);
    }

    hermes::Serializer s(dir.path());
    s.serialize(transaction_batch);
    s.serialize(event_batch);
    s.finalize();

    hermes::Loader loader(dir.path());
    auto tables = loader.get_transactions(0, num_transactions);
    EXPECT_EQ(tables.size(), 1);
    auto table = tables[0];
    auto t_batch = hermes::TransactionBatch::deserialize(table);
    auto const &transaction = (*t_batch)[42];
    auto events = loader.get_events(*transaction);
    EXPECT_EQ(events.size(), 42 % 10);
    EXPECT_NE(events[0], nullptr);
    auto v = events[0]->get_value<uint64_t>("value");
    EXPECT_TRUE(v);
    EXPECT_EQ(*v, 42);
}
