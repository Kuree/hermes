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
        e->add_value<bool>("value2", true);
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
    auto table = tables[0].table;
    auto event_batch = hermes::EventBatch::deserialize(table);
    EXPECT_EQ(event_batch->size(), num_event);
    auto const &event = (*event_batch)[42];
    EXPECT_EQ(event->time(), 42);
    auto value = event->get_value<uint64_t>("value1");
    EXPECT_TRUE(value);
    EXPECT_EQ(*value, 42);
    auto bool_value = event->get_value<bool>("value2");
    EXPECT_TRUE(bool_value);
    EXPECT_TRUE(*bool_value);
}

TEST(serialization, multiple_event_batches) {  // NOLINT
    TempDirectory dir;

    hermes::EventBatch batch;
    constexpr auto num_event = 1000;
    uint64_t i;
    for (i = 0; i < num_event; i++) {
        auto e = std::make_unique<hermes::Event>(i);
        e->add_value<uint64_t>("value1", i);
        batch.emplace_back(std::move(e));
    }

    EXPECT_TRUE(batch.validate());
    // serialize it
    hermes::Serializer s(dir.path());
    s.serialize(batch);
    batch.clear();

    for (; i < num_event * 2; i++) {
        auto e = std::make_unique<hermes::Event>(i);
        e->add_value<uint64_t>("value1", i);
        batch.emplace_back(std::move(e));
    }
    s.serialize(batch);

    s.finalize();

    hermes::Loader loader(dir.path());
    auto tables = loader.get_events(0, num_event * 2);
    EXPECT_EQ(tables.size(), 1);
    auto table = tables[0].table;
    auto event_batch = hermes::EventBatch::deserialize(table);
    EXPECT_EQ(event_batch->size(), num_event * 2);
    auto const &event = (*event_batch)[42 + num_event];
    EXPECT_EQ(event->time(), 42 + num_event);
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
    auto table = tables[0].table;
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
    auto table = tables[0].table;
    auto t_batch = hermes::TransactionBatch::deserialize(table);
    auto const &transaction = (*t_batch)[42];
    auto events = loader.get_events(*transaction);
    EXPECT_EQ(events.size(), 42 % 10);
    EXPECT_NE(events[0], nullptr);
    auto v = events[0]->get_value<uint64_t>("value");
    EXPECT_TRUE(v);
    EXPECT_EQ(*v, 42);
}


TEST(serilizaiton, transaction_stream) {  // NOLINT
    TempDirectory dir;

    hermes::TransactionBatch transaction_batch;
    transaction_batch.set_transaction_name("test");
    hermes::EventBatch event_batch;
    constexpr auto num_transactions = 1000;
    for (auto i = 0; i < num_transactions; i++) {
        auto t = std::make_shared<hermes::Transaction>(i);
        for (uint32_t j = 0; j < ((i % 10) + 1); j++) {
            auto e = std::make_shared<hermes::Event>(i);
            e->add_value<uint64_t>("value", i);
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
    auto stream = loader.get_transaction_stream("test");
    EXPECT_TRUE(stream);
    EXPECT_EQ(stream->size(), num_transactions);
    for (auto const &iter: *stream) {
        auto events = iter.events;
        EXPECT_GE(events->size(), 1);
    }
}
