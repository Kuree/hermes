#ifndef HERMES_TRACKER_HH
#define HERMES_TRACKER_HH

#include <unordered_set>

#include "pubsub.hh"
#include "serializer.hh"
#include "transaction.hh"

namespace hermes {

class Tracker : public Subscriber {
public:
    explicit Tracker(const std::string &topic);
    Tracker(MessageBus *bus, std::string topic);
    void connect();
    void set_serializer(const std::shared_ptr<Serializer> &serializer) { serializer_ = serializer; }
    void flush(bool save_inflight_transaction = false);

    Transaction *get_new_transaction();
    virtual Transaction *track(Event *event) = 0;

    const TransactionBatch &finished_transactions() const { return finished_transactions_; }
    [[maybe_unused]] void set_transaction_name(std::string transaction_name);
    const std::string &transaction_name() const { return transaction_name_; }

    void retire_transaction(const std::shared_ptr<Transaction> &transaction);

    ~Tracker() { flush(true); }
    void stop() override { flush(true); }

protected:
    void on_message(const std::string &, const std::shared_ptr<Event> &event) override;
    std::string transaction_name_;

private:
    std::string topic_;
    std::unordered_set<std::shared_ptr<Transaction>> inflight_transactions;
    // normally we don't flush finished transactions to disk unless relevant functions
    // are called, since transaction data are not that big.
    TransactionBatch finished_transactions_;

    // serializer
    std::shared_ptr<Serializer> serializer_;

    // how often to flush the transactions to files
    constexpr static uint64_t transaction_flush_threshold_ = 1 << 16;
};

extern void add_tracker_to_simulator(const std::shared_ptr<Tracker> &tracker);

}  // namespace hermes

#endif  // HERMES_TRACKER_HH
