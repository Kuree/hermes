#ifndef HERMES_TRACKER_HH
#define HERMES_TRACKER_HH

#include "transaction.hh"
#include "pubsub.hh"
#include <unordered_set>

namespace hermes {

class Tracker: public Subscriber {
public:
    explicit Tracker(const std::string &name);
    Tracker(MessageBus *bus, const std::string &name);

    Transaction * get_new_transaction();
    virtual Transaction *track(Event *event) = 0;

protected:
    void on_message(const std::string &, const std::shared_ptr<Event> &event) override;

private:
    std::string name_;
    std::unordered_set<std::shared_ptr<Transaction>> inflight_transactions;
    std::unordered_map<uint64_t, std::shared_ptr<Event>> inflight_events_;
    TransactionBatch finished_transactions_;
    // notice that in the current model, we assume that the event can only happens in one
    // transaction. although technically speaking the events can be stored in multiple
    // places since there is no constraint on id being unique
    EventBatch finished_events_;
};

}

#endif  // HERMES_TRACKER_HH
