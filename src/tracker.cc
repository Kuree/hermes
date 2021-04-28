#include "tracker.hh"

#include <utility>

namespace hermes {

void Tracker::on_message(const std::string &, const std::shared_ptr<Event> &event) {
    auto *event_ptr = event.get();
    track(event_ptr);
}

void GroupTracker::on_message(const std::string &,
                              const std::shared_ptr<Transaction> &transaction) {
    auto *ptr = transaction.get();
    track(ptr);
}

}  // namespace hermes