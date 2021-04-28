#include "tracker.hh"

class DummyTracker : public hermes::Tracker {
public:
    explicit DummyTracker(const std::string &topic) : hermes::Tracker(topic) {}

    void track(hermes::Event *event) override {
        if (event->id() % 10 == 0) {
            current_transaction_ = get_new_transaction();
        }
        current_transaction_->add_event(event);

        if ((event->id() % 10) == 9) {
            current_transaction_->finish();
        }
    }

private:
    hermes::Transaction *current_transaction_ = nullptr;
};

extern "C" {
[[maybe_unused]] void test_tracker_lib() {
    auto tracker = std::make_shared<DummyTracker>("*");
    hermes::add_tracker_to_simulator(tracker);
}
}