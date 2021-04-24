#include "logger.hh"

#include "serializer.hh"

namespace hermes {

DummyEventSerializer::DummyEventSerializer(std::string topic) : topic_(std::move(topic)) {
    priority_ = default_priority * 10;
}

void DummyEventSerializer::connect(MessageBus *bus, const std::shared_ptr<Serializer> &serializer) {
    subscribe(bus, topic_);
    serializer_ = serializer;
}

void DummyEventSerializer::on_message(const std::string &topic,
                                      const std::shared_ptr<Event> &event) {
    events_[topic].emplace_back(event);
    if (events_.at(topic).name().empty()) {
        events_.at(topic).set_name(topic);
    }

    if (serializer_ && events_.at(topic).size() >= event_dump_threshold) {
        serializer_->serialize(events_.at(topic));
        events_[topic].clear();
    }
}

void DummyEventSerializer::on_message(const std::string &topic,
                                      const std::shared_ptr<Transaction> &transaction) {
    transactions_[topic].emplace_back(transaction);

    if (transactions_.at(topic).name().empty()) {
        transactions_.at(topic).set_name(topic);
    }

    if (serializer_ && transactions_.at(topic).size() >= transaction_dump_threshold) {
        serializer_->serialize(transactions_.at(topic));
        transactions_[topic].clear();
    }
}

void DummyEventSerializer::on_message(const std::string &topic,
                                      const std::shared_ptr<TransactionGroup> &group) {
    transaction_groups_[topic].emplace_back(group);

    if (transaction_groups_.at(topic).name().empty()) {
        transaction_groups_.at(topic).set_name(topic);
    }

    if (serializer_ && transaction_groups_.at(topic).size() >= transaction_dump_threshold) {
        serializer_->serialize(transaction_groups_.at(topic));
        transaction_groups_[topic].clear();
    }
}

void DummyEventSerializer::flush() {
    if (!serializer_) return;

    // flush everything
    for (auto &[name, events] : events_) {
        if (!events.empty()) {
            serializer_->serialize(events);
            events.clear();
        }
    }

    for (auto &[name, transactions] : transactions_) {
        if (!transactions.empty()) {
            serializer_->serialize(transactions);
            transactions.clear();
        }
    }

    for (auto &[name, groups] : transaction_groups_) {
        if (!groups.empty()) {
            serializer_->serialize(groups);
            groups.clear();
        }
    }
}

void DummyEventSerializer::stop() {
    flush();
    Subscriber::stop();
}

static bool event_in_order_ = true;
void set_event_in_order(bool value) { event_in_order_ = value; }

bool event_in_order() { return event_in_order_; }

}  // namespace hermes