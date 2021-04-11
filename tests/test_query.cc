#include "gtest/gtest.h"
#include "logger.hh"
#include "pubsub.hh"
#include "query.hh"
#include "serializer.hh"
#include "test_util.hh"

class QueryHelperTest : public ::testing::Test, public EventTransactionInitializer {
    void SetUp() override { setup(); }
};

TEST_F(QueryHelperTest, concurrent_event) {  // NOLINT
    hermes::QueryHelper helper(loader);
    auto events = helper.concurrent_events(100);
    EXPECT_EQ(events.size(), 1);
    EXPECT_EQ(*events[0]->get_value<uint64_t>("value"), 100);
    events = helper.concurrent_events(events[0]);
    EXPECT_EQ(events[0]->time(), 100);

    events = helper.concurrent_events("test", 990, 1010);
    EXPECT_EQ(events.size(), 1010 - 990 + 1);
}

TEST_F(QueryHelperTest, concurrent_transaction) {  // NOLINT
    hermes::QueryHelper helper(loader);
    auto transactions = helper.concurrent_transactions(100);
    EXPECT_EQ(transactions.size(), 1);

    transactions = helper.concurrent_transactions("test", 42, 52);
    EXPECT_EQ(transactions.size(), 2);
}