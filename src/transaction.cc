#include "transaction.hh"

namespace hermes {

void Transaction::add_event(std::unique_ptr<Event> &event) {
    if (start_time_ > event->time()) {
        start_time_ = event->time();
    }
    if (end_time_ < event->time()) {
        end_time_ = event->time();
    }

    events_.emplace_back(std::move(event));
}

}  // namespace hermes