#include "arrow.hh"
#include "gtest/gtest.h"
#include "loader.hh"
#include "logger.hh"
#include "pubsub.hh"
#include "serializer.hh"
#include "test_util.hh"

class StreamSubSubscriber : public hermes::Subscriber {
public:
    void on_message(const std::string &, const std::shared_ptr<hermes::Event> &event) override {
        events.emplace_back(event);
        if (!event_time_) {
            event_time_ = event->time();
        } else {
            EXPECT_GT(event->time(), event_time_);
            event_time_ = event->time();
        }
        EXPECT_EQ(*event->get_value<uint32_t>("value"), event->time());
    }

    void on_message(const std::string &,
                    const std::shared_ptr<hermes::Transaction> &transaction) override {
        transactions.emplace_back(transaction);
    }

    std::vector<std::shared_ptr<hermes::Event>> events;
    std::vector<std::shared_ptr<hermes::Transaction>> transactions;
    std::optional<uint64_t> event_time_;
};

class LoaderTest : public ::testing::Test {
public:
    TempDirectory dir;
    hermes::Publisher publisher;

    // need to create s set of events and transactions
    static constexpr auto event_name = "dummy1";
    static constexpr auto chunk_size = 5;
    uint64_t num_events = 100;
    std::shared_ptr<hermes::Serializer> serializer;
    std::unique_ptr<hermes::FileSystemInfo> info = nullptr;

    void SetUp() override {
        if (!info) {
            serializer = std::make_shared<hermes::Serializer>(dir.path());
        } else {
            serializer = std::make_shared<hermes::Serializer>(*info, true);
        }

        auto d = std::make_shared<hermes::DummyEventSerializer>();
        d->connect(serializer);

        for (auto i = 0u; i < num_events; i++) {
            auto e = std::make_shared<hermes::Event>(i);
            e->add_value<uint32_t>("value", i);
            publisher.publish(event_name, e);
        }

        // flush it here since we're creating a new batch
        d->flush();

        for (auto i = num_events; i < num_events * 2; i++) {
            auto e = std::make_shared<hermes::Event>(i);
            e->add_value<uint32_t>("value", i);
            publisher.publish(event_name, e);
        }

        // we don't even track it, just directly use transactions
        auto id = 0u;
        for (auto i = 0; i < num_events * 2 / chunk_size; i++) {
            auto t = std::make_shared<hermes::Transaction>();
            for (auto j = 0; j < chunk_size; j++) {
                hermes::Event e(id);
                e.set_id(id++);
                t->add_event(&e);
            }
            publisher.publish(event_name, t);

            if (id == num_events) {
                // create second batch of transaction
                d->flush();
            }
        }

        hermes::MessageBus::default_bus()->stop();
        serializer->finalize();
    }
};

TEST_F(LoaderTest, stream) {  // NOLINT
    hermes::Loader loader(dir.path());
    auto sub = std::make_shared<StreamSubSubscriber>();
    sub->subscribe(hermes::MessageBus::default_bus(), event_name);
    loader.stream();
    EXPECT_EQ(sub->events.size(), 2 * num_events);
    EXPECT_EQ(sub->transactions.size(), num_events * 2 / chunk_size);
}

TEST_F(LoaderTest, names) {  // NOLINT
    hermes::Loader loader(dir.path());
    auto names = loader.get_event_names();
    EXPECT_EQ(names.size(), 1);
    names = loader.get_transaction_names();
    EXPECT_EQ(names.size(), 1);
    names = loader.get_transaction_group_names();
    EXPECT_EQ(names.size(), 0);

    // test schema as well
    auto schema = loader.get_event_schema(event_name);
    EXPECT_FALSE(schema.empty());
    EXPECT_EQ(schema.at(hermes::Event::NAME_NAME), hermes::EventDataType::string);
}

TEST_F(LoaderTest, stream_iter) {  // NOLINT
    hermes::Loader loader(dir.path());
    auto stream = loader.get_transaction_stream(event_name);
    uint64_t num_trans = 0;
    for (auto const &[transaction, events, _] : *stream) {
        EXPECT_EQ(transaction->id(), num_trans);
        EXPECT_EQ(events->size(), chunk_size);
        for (auto const &event : *events) {
            EXPECT_NE(event, nullptr);
        }
        num_trans++;
    }
    EXPECT_EQ(num_trans, num_events * 2 / chunk_size);
}

TEST_F(LoaderTest, filter_stream_iter) {  // NOLINT
    hermes::Loader loader(dir.path());
    auto stream = loader.get_transaction_stream(event_name);
    auto filtered_stream = stream->where([](const hermes::TransactionData &data) -> bool {
        return data.transaction->id() % 2 == 0;
    });
    EXPECT_EQ(filtered_stream.size(), stream->size() / 2);
    // check ordering as well
    std::vector<uint64_t> ids;
    ids.reserve(filtered_stream.size());
    for (auto const &data: filtered_stream) {
        ids.emplace_back(data.transaction->id());
    }
    for (uint64_t i = 0; i < ids.size(); i++) {
        EXPECT_EQ(ids[i], i * 2);
    }
}

TEST_F(LoaderTest, filter_stream_iter_cascade) { // NOLINT
    hermes::Loader loader(dir.path());
    auto stream = loader.get_transaction_stream(event_name);
    auto filtered_stream = stream->where([](const hermes::TransactionData &data) -> bool {
      return data.transaction->id() % 2 == 0;
    });
    EXPECT_EQ(filtered_stream.size(), stream->size() / 2);
    auto second_filtered_stream = filtered_stream.where([](const hermes::TransactionData &data) -> bool {
        return data.transaction->id() % 4 == 0;
    });
    EXPECT_EQ(second_filtered_stream.size(), stream->size() / 4);
    // check ordering as well
    std::vector<uint64_t> ids;
    ids.reserve(second_filtered_stream.size());
    for (auto const &data: second_filtered_stream) {
        ids.emplace_back(data.transaction->id());
    }
    for (uint64_t i = 0; i < ids.size(); i++) {
        EXPECT_EQ(ids[i], i * 4);
    }
}

class S3LoaderTest : public LoaderTest {
    void SetUp() override {
        // only if the port is open
        bool localstack_running = is_port_open(4566);
        if (!localstack_running) GTEST_SKIP_("localstack not running");
        info = std::make_unique<hermes::FileSystemInfo>("s3://test/test");
        info->end_point = "http://localhost:4566";
        info->secret_key = "test";
        info->access_key = "test";
        LoaderTest::SetUp();
    }
};

TEST_F(S3LoaderTest, stream) {  // NOLINT
    hermes::Loader loader({*info});
    auto stream = loader.get_transaction_stream(event_name);
    uint64_t num_trans = 0;
    for (auto const &[transaction, events, _] : *stream) {
        EXPECT_EQ(transaction->id(), num_trans);
        EXPECT_EQ(events->size(), chunk_size);
        for (auto const &event : *events) {
            EXPECT_NE(event, nullptr);
        }
        num_trans++;
    }
    EXPECT_EQ(num_trans, num_events * 2 / chunk_size);
}

#ifdef PERFORMANCE_TEST

class LoaderPerformanceTest : public LoaderTest {
    void SetUp() override {
        num_events = 100000;
        LoaderTest::SetUp();
    }
};

TEST_F(LoaderPerformanceTest, events_stream) {  // NOLINT
    hermes::Loader loader(dir.path());
    auto stream = loader.get_transaction_stream(event_name);
    uint64_t num_trans = 0;
    for (auto const &[transaction, events, _] : *stream) {
        (void)transaction;
        for (auto const &event : *events) {
            EXPECT_NE(event, nullptr);
        }
        num_trans++;
    }
    EXPECT_GT(num_trans, 0);
}

#endif