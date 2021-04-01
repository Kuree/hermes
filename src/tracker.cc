#include "tracker.hh"

#include <utility>

namespace hermes {

Tracker::Tracker(MessageBus *bus, std::string topic) : topic_(std::move(topic)) { bus_ = bus; }

Tracker::Tracker(const std::string &topic) : Tracker(MessageBus::default_bus(), topic) {}

void Tracker::connect() { subscribe(bus_, topic_); }

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
            finished_transactions_.reserve(inflight_transactions.size());
            for (auto const &transaction : inflight_transactions) {
                finished_transactions_.emplace_back(transaction);
            }
            serializer_->serialize(finished_transactions_);
            inflight_transactions.clear();
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

[[maybe_unused]] void Tracker::set_transaction_name(std::string transaction_name) {
    transaction_name_ = std::move(transaction_name);

    finished_transactions_.set_transaction_name(transaction_name_);
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