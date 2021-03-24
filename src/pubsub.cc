#include "pubsub.hh"

namespace hermes {

void MessageBus::publish(const std::string &topic, const std::shared_ptr<Event> &event) {
    if (subscribers_.find(topic) != subscribers_.end()) {
        auto &subs = subscribers_.at(topic);
        for (auto const &sub : subs) {
            sub->on_message(topic, event);
        }
    }
}

void MessageBus::publish(const std::string &topic, const std::shared_ptr<Transaction> &event) {
    if (subscribers_.find(topic) != subscribers_.end()) {
        auto &subs = subscribers_.at(topic);
        for (auto const &sub : subs) {
            sub->on_message(topic, event);
        }
    }
}

void MessageBus::add_subscriber(const std::string &topic, std::shared_ptr<Subscriber> &subscriber) {
    subscribers_[topic].emplace(subscriber);
}

bool Publisher::publish(const std::string &topic, const std::shared_ptr<Event> &event) {
    if (bus_) {
        bus_->publish(topic, event);
        return true;
    }
    return false;
}

bool Publisher::publish(const std::string &topic, const std::shared_ptr<Transaction> &transaction) {
    if (bus_) {
        bus_->publish(topic, transaction);
        return true;
    }
    return false;
}

void Subscriber::subscribe(MessageBus *bus, const std::string &topic) {
    auto ptr = shared_from_this();
    bus->add_subscriber(topic, ptr);
}

}  // namespace hermes