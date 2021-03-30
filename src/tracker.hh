#ifndef HERMES_TRACKER_HH
#define HERMES_TRACKER_HH

#include <unordered_set>

#include "pubsub.hh"
#include "serializer.hh"
#include "transaction.hh"

namespace hermes {

class Tracker : public Subscriber {
public:
    explicit Tracker(const std::string &name);
    Tracker(MessageBus *bus, std::string name);
    void connect();
    void set_serializer(Serializer *serializer) { serializer_ = serializer; }
    void flush();

    Transaction *get_new_transaction();
    virtual Transaction *track(Event *event) = 0;

    const TransactionBatch &finished_transactions() const { return finished_transactions_; }

    ~Tracker() { flush(); }
    void stop() override { flush(); }

protected:
    void on_message(const std::string &, const std::shared_ptr<Event> &event) override;

private:
    std::string name_;
    std::unordered_set<std::shared_ptr<Transaction>> inflight_transactions;
    // normally we don't flush finished transactions to disk unless relevant functions
    // are called, since transaction data are not that big.
    TransactionBatch finished_transactions_;

    // serializer
    Serializer *serializer_ = nullptr;
};

extern void add_tracker_to_simulator(const std::shared_ptr<Tracker> &tracker);

}  // namespace hermes

#endif  // HERMES_TRACKER_HH
