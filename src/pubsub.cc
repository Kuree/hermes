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

void MessageBus::add_subscriber(const std::string &topic, std::shared_ptr<Subscriber> &subscriber) {
    subscribers_[topic].emplace(subscriber);
}

MessageBus *MessageBus::default_bus() {
    static MessageBus bus;
    return &bus;
}

std::set<std::shared_ptr<Subscriber>> MessageBus::get_subscribers() const {
    std::set<std::shared_ptr<Subscriber>> result;
    for (auto const &[topic, subs] : subscribers_) {
        for (auto const &sub : subs) result.emplace(sub);
    }
    return result;
}

void MessageBus::stop() const {
    auto subs = get_subscribers();
    for (const auto &sub : subs) {
        sub->stop();
    }
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

void DummyEventSerializer::connect(MessageBus *bus, Serializer *serializer) {
    subscribe(bus, topic_);
    serializer_ = serializer;
}

void DummyEventSerializer::on_message(const std::string &topic,
                                      const std::shared_ptr<Event> &event) {
    events_[topic].emplace_back(event);
    if (serializer_ && events_.at(topic).size() >= event_dump_threshold) {
        serializer_->serialize(events_.at(topic));
        events_[topic].clear();
    }
}

void DummyEventSerializer::stop() {
    if (!serializer_) return;

    // flush everything
    for (auto &[name, events] : events_) {
        if (!events.empty()) {
            serializer_->serialize(events);
            events.clear();
        }
    }
}

}  // namespace hermes