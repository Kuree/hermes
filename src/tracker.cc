#include "tracker.hh"

namespace hermes {

Tracker::Tracker(MessageBus *bus, const std::string &name) : { subscribe(bus, name); }

Tracker::Tracker(const std::string &name) : Tracker(MessageBus::default_bus(), name) {}

Transaction *Tracker::get_new_transaction() {
    // we use the default id allocator
    auto t = std::make_unique<Transaction>();
    auto *ptr = t.get();
    inflight_transactions.emplace(std::move(t));
    return ptr;
}

void Tracker::on_message(const std::string &, const std::shared_ptr<Event> &event) {
    auto *event_ptr = event.get();
    auto *t = track(event_ptr);
}


}  // namespace hermes