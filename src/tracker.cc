#include "tracker.hh"

#include <utility>

namespace hermes {

void Tracker::on_message(const std::string &, const std::shared_ptr<Event> &event) {
    auto *event_ptr = event.get();
    auto *t = track(event_ptr);
    if (t) {
        if (t->finished()) {
            auto transaction = t->shared_from_this();
            retire_transaction(transaction);
        }
    }
}

void GroupTracker::on_message(const std::string &,
                              const std::shared_ptr<Transaction> &transaction) {
    auto *ptr = transaction.get();
    auto *t = track(ptr);
    if (t) {
        if (t->finished()) {
            auto trans = t->shared_from_this();
            retire_transaction(trans);
        }
    }
}

}  // namespace hermes