#include "gtest/gtest.h"
#include "tracker.hh"
#include "test_util.hh"
#include "loader.hh"

class DummyTracker : public hermes::Tracker {
public:
    DummyTracker(const std::string& name, uint64_t chunks)
        : hermes::Tracker(name), chunks_(chunks) {}

    hermes::Transaction* track(hermes::Event* event) override {
        auto r = event->id() % chunks_;
        if (r == 0) {
            return nullptr;
        } else {
            if (r == 1) {
                // new transaction
                auto* t = get_new_transaction();
                t->add_event(event);
                current_transaction_ = t;
            } else if (r == (chunks_ - 1)) {
                // finish this transaction
                current_transaction_->add_event(event);
                current_transaction_->finish();
            } else {
                current_transaction_->add_event(event);
            }
            return current_transaction_;
        }
    }

private:
    uint64_t chunks_;
    hermes::Transaction* current_transaction_ = nullptr;
};

TEST(tracker, dummy_tracker) { // NOLINT
    constexpr auto event_name = "dummy";
    constexpr auto chunk_size = 5;
    constexpr auto num_events = 100;
    auto tracker = std::make_shared<DummyTracker>(event_name, chunk_size);
    tracker->connect();
    hermes::Publisher publisher;

    for (auto i = 0u; i < num_events; i++) {
        auto e = std::make_shared<hermes::Event>(i);
        e->add_value<uint32_t>("value", i);
        publisher.publish(event_name, e);
    }

    auto finished_transactions = tracker->finished_transactions();
    EXPECT_EQ(finished_transactions.size(), num_events / chunk_size);

    for (auto const &transaction: finished_transactions) {
        EXPECT_EQ(transaction->events().size(), chunk_size - 1);
    }
}


TEST(tracker, dummy_tracker_flush) { // NOLINT
    TempDirectory dir;
    constexpr auto event_name = "dummy";
    constexpr auto chunk_size = 5;
    constexpr auto num_events = 100;
    auto tracker = std::make_shared<DummyTracker>(event_name, chunk_size);
    hermes::Serializer serializer(dir.path());
    tracker->connect();
    tracker->set_serializer(&serializer);
    hermes::Publisher publisher;

    for (auto i = 0u; i < num_events; i++) {
        auto e = std::make_shared<hermes::Event>(i);
        e->add_value<uint32_t>("value", i);
        publisher.publish(event_name, e);
    }

    auto finished_transactions = tracker->finished_transactions();
    EXPECT_EQ(finished_transactions.size(), num_events / chunk_size);

    for (auto const &transaction: finished_transactions) {
        EXPECT_EQ(transaction->events().size(), chunk_size - 1);
    }

    tracker->flush();

    // load files
    hermes::Loader loader(dir.path());
    auto events = loader.get_events(0, 20);
    EXPECT_FALSE(events.empty());

}
