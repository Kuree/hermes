#ifndef HERMES_TRACKER_HH
#define HERMES_TRACKER_HH

#include "transaction.hh"
#include "pubsub.hh"
#include "serializer.hh"
#include <unordered_set>

namespace hermes {

class Tracker: public Subscriber {
public:
    explicit Tracker(const std::string &name);
    Tracker(MessageBus *bus, std::string name);
    void connect();
    void set_serializer(Serializer *serializer) { serializer_ = serializer; }
    void flush();
    void set_event_flush_threshold(uint64_t value) { event_flush_threshold_ = value; }

    Transaction * get_new_transaction();
    virtual Transaction *track(Event *event) = 0;

    const TransactionBatch &finished_transactions() const { return finished_transactions_; }

    ~Tracker() { flush(); }

protected:
    void on_message(const std::string &, const std::shared_ptr<Event> &event) override;
    uint64_t event_flush_threshold_ = 1u << 20u;

private:
    std::string name_;
    std::unordered_set<std::shared_ptr<Transaction>> inflight_transactions;
    std::unordered_map<uint64_t, std::shared_ptr<Event>> inflight_events_;
    // normally we don't flush finished transactions to disk unless relevant functions
    // are called, since transaction data are not that big.
    TransactionBatch finished_transactions_;
    // notice that in the current model, we assume that the event can only happens in one
    // transaction. although technically speaking the events can be stored in multiple
    // places since there is no constraint on id being unique
    EventBatch finished_events_;

    // serializer
    Serializer *serializer_ = nullptr;
};

}

#endif  // HERMES_TRACKER_HH
