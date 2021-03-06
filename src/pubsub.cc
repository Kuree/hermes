#include "pubsub.hh"

#include <fnmatch.h>

#include "serializer.hh"

namespace hermes {

void MessageBus::publish(const std::string &topic, const std::shared_ptr<Event> &event) {
    for (auto const &[name, subs] : subscribers_) {
        if (fnmatch(name.c_str(), topic.c_str(), FNM_EXTMATCH) == 0) {
            // this is a match
            for (auto const &sub : subs) {
                sub->on_message(topic, event);
            }
        }
    }
}

void MessageBus::publish(const std::string &topic,
                         const std::shared_ptr<Transaction> &transaction) {
    for (auto const &[name, subs] : subscribers_) {
        if (fnmatch(name.c_str(), topic.c_str(), FNM_EXTMATCH) == 0) {
            // this is a match
            for (auto const &sub : subs) {
                sub->on_message(topic, transaction);
            }
        }
    }
}

void MessageBus::publish(const std::string &topic, const std::shared_ptr<TransactionGroup> &group) {
    for (auto const &[name, subs] : subscribers_) {
        if (fnmatch(name.c_str(), topic.c_str(), FNM_EXTMATCH) == 0) {
            // this is a match
            for (auto const &sub : subs) {
                sub->on_message(topic, group);
            }
        }
    }
}

void MessageBus::add_subscriber(const std::string &topic,
                                const std::shared_ptr<Subscriber> &subscriber) {
    subscribers_[topic].emplace(subscriber);
}

void MessageBus::unsubscribe(const std::shared_ptr<Subscriber> &sub) {
    for (auto &iter : subscribers_) {
        iter.second.erase(sub);
    }
}

MessageBus *MessageBus::default_bus() {
    static std::shared_ptr<MessageBus> bus;
    if (!bus) {
        bus = std::make_shared<MessageBus>();
    }
    return bus.get();
}

std::set<std::shared_ptr<Subscriber>> MessageBus::get_subscribers() const {
    std::set<std::shared_ptr<Subscriber>> result;
    for (auto const &[topic, subs] : subscribers_) {
        for (auto const &sub : subs) result.emplace(sub);
    }
    return result;
}

const MessageBus::SubList *MessageBus::get_subscribers(const std::string &topic) const {
    if (subscribers_.find(topic) != subscribers_.end()) {
        return &subscribers_.at(topic);
    } else {
        return nullptr;
    }
}

void MessageBus::stop() const {
    auto subs = get_subscribers();
    for (const auto &sub : subs) {
        sub->stop();
    }
}

bool MessageBus::SubCmp::operator()(const std::shared_ptr<Subscriber> &a,
                                    const std::shared_ptr<Subscriber> &b) const {
    // we need custom comparator to allow strict week ordering based on priority as well as
    // pointer address
    if (a == b) return false;
    if (a->priority() != b->priority()) {
        return a->priority() < b->priority();
    } else {
        // use pointer address
        return a < b;
    }
}

bool Publisher::publish(const std::string &topic, const std::shared_ptr<Event> &event) {
    if (bus_) {
        bus_->publish(topic, event);
        return true;
    }
    return false;
}

bool Publisher::publish(const std::string &topic, const std::shared_ptr<TransactionGroup> &group) {
    if (bus_) {
        bus_->publish(topic, group);
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

void Subscriber::stop() {
    if (bus_) {
        bus_->unsubscribe(shared_from_this());
    }
}

}  // namespace hermes