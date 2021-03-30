#ifndef HERMES_PUBSUB_HH
#define HERMES_PUBSUB_HH

#include <set>

#include "transaction.hh"

namespace hermes {

class Publisher;
class Subscriber;

class MessageBus {
public:
    void publish(const std::string &topic, const std::shared_ptr<Event> &event);
    void publish(const std::string &topic, const std::shared_ptr<Transaction> &transaction);
    void add_subscriber(const std::string &topic, std::shared_ptr<Subscriber> &subscriber);

    static MessageBus *default_bus();
    std::set<std::shared_ptr<Subscriber>> get_subscribers() const;
    void stop() const;

private:
    std::unordered_map<std::string, std::set<std::shared_ptr<Subscriber>>> subscribers_;
};

class Publisher {
public:
    explicit Publisher(MessageBus *bus) : bus_(bus) {}
    Publisher() : bus_(MessageBus::default_bus()) {}

    bool publish(const std::string &topic, const std::shared_ptr<Transaction> &transaction);
    bool publish(const std::string &topic, const std::shared_ptr<Event> &event);

private:
    MessageBus *bus_;
};

class Subscriber : public std::enable_shared_from_this<Subscriber> {
public:
    Subscriber() = default;
    void subscribe(MessageBus *bus, const std::string &topic);
    virtual void stop() {}

protected:
    virtual void on_message(const std::string &topic, const std::shared_ptr<Event> &event) {}
    virtual void on_message(const std::string &topic,
                            const std::shared_ptr<Transaction> &transaction) {}

    MessageBus *bus_ = nullptr;

private:
    friend MessageBus;
};

// convenient way to store all events
class Serializer;
class DummyEventSerializer : public Subscriber {
public:
    DummyEventSerializer() : DummyEventSerializer("*") {}
    explicit DummyEventSerializer(std::string topic) : topic_(std::move(topic)) {}
    void connect(Serializer *serializer) { connect(MessageBus::default_bus(), serializer); }
    void connect(MessageBus *bus, Serializer *serializer);
    void on_message(const std::string &topic, const std::shared_ptr<Event> &event) override;
    void stop() override;

private:
    std::string topic_;
    Serializer *serializer_ = nullptr;
    static constexpr uint64_t event_dump_threshold = 1 << 15;

    std::map<std::string, EventBatch> events_;
};

}  // namespace hermes

#endif  // HERMES_PUBSUB_HH
