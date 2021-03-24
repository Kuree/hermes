#ifndef HERMES_PUBSUB_HH
#define HERMES_PUBSUB_HH

#include "transaction.hh"

namespace hermes {

class Publisher;
class Subscriber;

class MessageBus {
public:



};

class Publisher {
public:
    explicit Publisher(MessageBus *bus): bus_(bus) {}

    bool publish(const std::string &topic, const std::shared_ptr<Transaction> &transaction);
    bool publish(const std::string &topic, const std::shared_ptr<Event> &event);

private:
    MessageBus *bus_;
};

class Subscriber: std::enable_shared_from_this<Subscriber> {
public:
    template<typename T>
    bool subscribe(MessageBus *bus, const std::string &topic);

private:
    MessageBus *bus_ = nullptr;
};

}

#endif  // HERMES_PUBSUB_HH
