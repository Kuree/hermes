#include "logger.hh"

#include "serializer.hh"

namespace hermes {

void DummyEventSerializer::connect(MessageBus *bus, const std::shared_ptr<Serializer> &serializer) {
    subscribe(bus, topic_);
    serializer_ = serializer;
}

void DummyEventSerializer::on_message(const std::string &topic,
                                      const std::shared_ptr<Event> &event) {
    events_[topic].emplace_back(event);
    if (events_.at(topic).event_name().empty()) {
        events_.at(topic).set_event_name(topic);
    }

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