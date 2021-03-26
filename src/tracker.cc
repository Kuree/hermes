#include "tracker.hh"

#include <utility>

namespace hermes {

Tracker::Tracker(MessageBus *bus, std::string name) : name_(std::move(name)) { bus_ = bus; }

Tracker::Tracker(const std::string &name) : Tracker(MessageBus::default_bus(), name) {}

void Tracker::connect() { subscribe(bus_, name_); }

Transaction *Tracker::get_new_transaction() {
    // we use the default id allocator
    auto t = std::make_shared<Transaction>();
    auto *ptr = t.get();
    inflight_transactions.emplace(t);
    return ptr;
}

void Tracker::on_message(const std::string &, const std::shared_ptr<Event> &event) {
    auto *event_ptr = event.get();
    auto *t = track(event_ptr);
    if (t) {
        if (t->finished()) {
            // if we finish this transaction
            // we remove inflight event and transaction to the finished bash
            for (auto const id : t->events()) {
                if (inflight_events_.find(id) != inflight_events_.end()) {
                    auto e = inflight_events_.at(id);
                    inflight_events_.erase(id);
                    finished_events_.emplace_back(e);
                }
            }
            finished_events_.emplace_back(event);
            auto transaction = t->shared_from_this();
            inflight_transactions.erase(transaction);
            finished_transactions_.emplace_back(transaction);
        } else {
            // add the current event to inflight events
            inflight_events_.emplace(event->id(), event);
        }
    }
}

}  // namespace hermes