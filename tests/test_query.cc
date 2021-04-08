#include "gtest/gtest.h"
#include "loader.hh"
#include "logger.hh"
#include "pubsub.hh"
#include "serializer.hh"
#include "test_util.hh"
#include "query.hh"

class QueryHelperTest : public ::testing::Test {
public:
    std::shared_ptr<hermes::Loader> loader;
    std::shared_ptr<TempDirectory> temp;

    void SetUp() override {
        temp = std::make_shared<TempDirectory>();
        // write out events and transactions
        auto serializer = std::make_shared<hermes::Serializer>(temp->path());
        auto dummy = std::make_shared<hermes::DummyEventSerializer>();
        dummy->connect(serializer);

        auto logger = std::make_shared<hermes::Logger>("test");

        uint64_t time = 0;
        // couple batches
        for (auto b = 0; b < 4; b++) {
            std::shared_ptr<hermes::Transaction> transaction;
            for (auto i = 0; i < 1000; i++) {
                auto event = std::make_shared<hermes::Event>(time++);
                event->add_value<uint64_t>("value", i);
                logger->log(event);
                if (i % 10 == 0) transaction = std::make_shared<hermes::Transaction>();
                transaction->add_event(event);
                if (i % 10 == 9) {
                    transaction->finish();
                    logger->log(transaction);
                }
            }
            // flush it
            dummy->flush();
        }

        hermes::MessageBus::default_bus()->stop();
        serializer->finalize();

        loader = std::make_shared<hermes::Loader>(temp->path());
    }
};

TEST_F(QueryHelperTest, concurrent_event) { // NOLINT
    hermes::QueryHelper helper(loader);
    auto events = helper.concurrent_events(100);
    EXPECT_EQ(events.size(), 1);
    EXPECT_EQ(*events[0]->get_value<uint64_t>("value"), 100);
    events = helper.concurrent_events(events[0]);
    EXPECT_EQ(events[0]->time(), 100);

    events = helper.concurrent_events("test", 990, 1010);
    EXPECT_EQ(events.size(), 1010 - 990 + 1);
}

TEST_F(QueryHelperTest, concurrent_transaction) { // NOLINT
    hermes::QueryHelper helper(loader);
    auto transactions = helper.concurrent_transactions(100);
    EXPECT_EQ(transactions.size(), 1);

    transactions = helper.concurrent_transactions("test", 42, 52);
    EXPECT_EQ(transactions.size(), 2);
}