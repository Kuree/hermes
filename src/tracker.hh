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
    std::unordered_set<std::unique_ptr<Transaction>> inflight_transactions;
    TransactionBatch finished_transactions_;
};

}

#endif  // HERMES_TRACKER_HH
