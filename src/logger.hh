#ifndef HERMES_LOGGER_HH
#define HERMES_LOGGER_HH

#include "event.hh"
#include "pubsub.hh"

namespace hermes {
class Logger : Publisher {
public:
    explicit Logger(std::string topic) : Publisher(), topic_(std::move(topic)) {}
    Logger(MessageBus *bus, std::string name) : Publisher(bus), topic_(std::move(name)) {}

    void log(const std::shared_ptr<Event> &event) { publish(topic_, event); }

protected:
    std::string topic_;
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

#endif  // HERMES_LOGGER_HH
