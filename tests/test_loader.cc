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
    static constexpr auto num_events = 100;
    std::shared_ptr<hermes::Serializer> serializer;

    void SetUp() override {
        serializer = std::make_shared<hermes::Serializer>(dir.path());

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
        for (auto i = 0; i < num_events * 2 / chunk_size; i++) {
            auto t = std::make_shared<hermes::Transaction>();
            for (auto j = 0; j < chunk_size; j++) {
                hermes::Event e(i * j);
                t->add_event(&e);
            }
            publisher.publish(event_name, t);
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

TEST_F(LoaderTest, stream_iter) {  // NOLINT
    hermes::Loader loader(dir.path());
    auto stream = loader.get_transaction_stream(event_name);
    uint64_t num_trans = 0;
    for (auto const &[transaction, events]: *stream) {
        EXPECT_EQ(transaction->id(), num_trans);
        EXPECT_EQ(events->size(), chunk_size);
        num_trans++;
    }
    EXPECT_EQ(num_trans, num_events * 2 / chunk_size);
}