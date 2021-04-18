#ifndef HERMES_LOGGER_HH
#define HERMES_LOGGER_HH

#include "event.hh"
#include "pubsub.hh"
#include "transaction.hh"

namespace hermes {
class Logger : Publisher {
public:
    explicit Logger(std::string topic) : Publisher(), topic_(std::move(topic)) {}
    Logger(MessageBus *bus, std::string name) : Publisher(bus), topic_(std::move(name)) {}

    void log(const std::shared_ptr<Event> &event) { publish(topic_, event); }
    void log(const std::shared_ptr<Transaction> &transaction) { publish(topic_, transaction); }
    void log(const std::shared_ptr<TransactionGroup> &group) { publish(topic_, group); }

    void log(const std::string &topic, const std::shared_ptr<Transaction> &transaction) {
        publish(topic, transaction);
    }
    void log(const std::string &topic, const std::shared_ptr<Event> &event) {
        publish(topic, event);
    }
    void log(const std::string &topic, const std::shared_ptr<TransactionGroup> &group) {
        publish(topic, group);
    }

protected:
    std::string topic_;
};

// convenient way to store all events
class Serializer;
class DummyEventSerializer : public Subscriber {
public:
    DummyEventSerializer() : DummyEventSerializer("*") {}
    explicit DummyEventSerializer(std::string topic);
    void connect(const std::shared_ptr<Serializer> &serializer) {
        connect(MessageBus::default_bus(), serializer);
    }
    void connect(MessageBus *bus, const std::shared_ptr<Serializer> &serializer);
    void on_message(const std::string &topic, const std::shared_ptr<Event> &event) override;
    void on_message(const std::string &topic,
                    const std::shared_ptr<Transaction> &transaction) override;
    void on_message(const std::string &topic,
                    const std::shared_ptr<TransactionGroup> &group) override;
    void stop() override;
    void flush();

private:
    std::string topic_;
    std::shared_ptr<Serializer> serializer_;
    static constexpr uint64_t event_dump_threshold = 1 << 15;

    std::map<std::string, EventBatch> events_;
    std::map<std::string, TransactionBatch> transactions_;
    std::map<std::string, TransactionGroupBatch> transaction_groups_;
};

}  // namespace hermes

#endif  // HERMES_LOGGER_HH
