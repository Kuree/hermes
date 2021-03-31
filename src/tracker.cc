#include "tracker.hh"

#include <utility>

namespace hermes {

Tracker::Tracker(MessageBus *bus, std::string name) : name_(std::move(name)) { bus_ = bus; }

Tracker::Tracker(const std::string &name) : Tracker(MessageBus::default_bus(), name) {}

void Tracker::connect() { subscribe(bus_, name_); }

void Tracker::flush(bool save_inflight_transaction) {
    if (!serializer_) return;
    if (!finished_transactions_.empty()) {
        serializer_->serialize(finished_transactions_);
        finished_transactions_.clear();
    }

    if (save_inflight_transaction) {
        if (!inflight_transactions.empty()) {
            // we reuse the finished transaction so that it will be saved in the
            // same places
            for (auto const &transaction : inflight_transactions) {
                finished_transactions_.emplace_back(transaction);
            }
            serializer_->serialize(finished_transactions_);
            finished_transactions_.clear();
        }
    }
}

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
            auto transaction = t->shared_from_this();
            inflight_transactions.erase(transaction);
            finished_transactions_.emplace_back(transaction);
        }
    }
}

}  // namespace hermes