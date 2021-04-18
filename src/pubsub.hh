#ifndef HERMES_PUBSUB_HH
#define HERMES_PUBSUB_HH

#include <set>

#include "transaction.hh"

namespace hermes {

class Publisher;
class Subscriber;

class MessageBus : public std::enable_shared_from_this<MessageBus> {
public:
    void publish(const std::string &topic, const std::shared_ptr<Event> &event);
    void publish(const std::string &topic, const std::shared_ptr<Transaction> &transaction);
    void publish(const std::string &topic, const std::shared_ptr<TransactionGroup> &group);
    void add_subscriber(const std::string &topic, const std::shared_ptr<Subscriber> &subscriber);
    void unsubscribe(const std::shared_ptr<Subscriber> &sub);

    static MessageBus *default_bus();
    std::set<std::shared_ptr<Subscriber>> get_subscribers() const;
    struct SubCmp {
        bool operator()(const std::shared_ptr<Subscriber> &a,
                        const std::shared_ptr<Subscriber> &b) const;
    };

    using SubList = std::set<std::shared_ptr<Subscriber>, SubCmp>;
    const SubList * get_subscribers(const std::string &topic) const;
    void stop() const;

private:

    std::unordered_map<std::string, SubList> subscribers_;
};

class Publisher {
public:
    explicit Publisher(MessageBus *bus) : bus_(bus) {}
    Publisher() : bus_(MessageBus::default_bus()) {}

    bool publish(const std::string &topic, const std::shared_ptr<Transaction> &transaction);
    bool publish(const std::string &topic, const std::shared_ptr<Event> &event);
    bool publish(const std::string &topic, const std::shared_ptr<TransactionGroup> &group);

private:
    MessageBus *bus_;
};

class Subscriber : public std::enable_shared_from_this<Subscriber> {
public:
    Subscriber() = default;
    void subscribe(MessageBus *bus, const std::string &topic);
    virtual void stop();
    [[nodiscard]] uint64_t priority() const { return priority_; }
    void set_priority(uint64_t value) { priority_ = value; }

    static constexpr uint64_t default_priority = 100;

protected:
    virtual void on_message(const std::string &topic, const std::shared_ptr<Event> &event) {}
    virtual void on_message(const std::string &topic,
                            const std::shared_ptr<Transaction> &transaction) {}
    virtual void on_message(const std::string &topic,
                            const std::shared_ptr<TransactionGroup> &group) {}

    MessageBus *bus_ = nullptr;

    uint64_t priority_ = default_priority;

private:
    friend MessageBus;
};

}  // namespace hermes

#endif  // HERMES_PUBSUB_HH
