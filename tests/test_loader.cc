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

TEST(loader, stream) {  // NOLINT
    TempDirectory dir;

    // need to create s set of events and transactions
    constexpr auto event_name = "dummy1";
    constexpr auto chunk_size = 5;
    constexpr auto num_events = 100;
    auto serializer = std::make_shared<hermes::Serializer>(dir.path());
    hermes::Publisher publisher;
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

    // now load it up
    hermes::Loader loader(dir.path());
    auto sub = std::make_shared<StreamSubSubscriber>();
    sub->subscribe(hermes::MessageBus::default_bus(), event_name);
    loader.stream();
    EXPECT_EQ(sub->events.size(),2 * num_events);
    EXPECT_EQ(sub->transactions.size(), num_events * 2 / chunk_size);
}