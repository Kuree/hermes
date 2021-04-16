#ifndef HERMES_TRACKER_HH
#define HERMES_TRACKER_HH

#include <unordered_set>

#include "pubsub.hh"
#include "serializer.hh"
#include "transaction.hh"

namespace hermes {

class TrackerBase: public Subscriber {
public:
    explicit TrackerBase(const std::string &topic);
    TrackerBase(MessageBus *bus, std::string topic);
    void connect();
    void set_serializer(const std::shared_ptr<Serializer> &serializer) { serializer_ = serializer; }
    virtual void flush() = 0;
    virtual void flush(bool save_inflight_transaction) = 0;

    [[maybe_unused]] void set_transaction_name(std::string transaction_name);
    const std::string &transaction_name() const { return transaction_name_; }

    virtual void retire_transaction(const std::shared_ptr<Transaction> &transaction) = 0;

    virtual ~TrackerBase() = default;
    void stop() override { flush(true); }

protected:
    std::string transaction_name_;
    // how often to flush the transactions to files
    constexpr static uint64_t transaction_flush_threshold_ = 1 << 16;
    // serializer
    std::shared_ptr<Serializer> serializer_;

private:
    std::string topic_;

};

class Tracker : public TrackerBase {
public:
    explicit Tracker(const std::string &topic): TrackerBase(topic) {}
    Tracker(MessageBus *bus, std::string topic): TrackerBase(bus, std::move(topic)) {}
    void flush() override { flush(true); }
    void flush(bool save_inflight_transaction) override { flush_(save_inflight_transaction); }

    Transaction *get_new_transaction();
    virtual Transaction *track(Event *event) = 0;

    const TransactionBatch &finished_transactions() const { return finished_transactions_; }

    void retire_transaction(const std::shared_ptr<Transaction> &transaction) override;

    ~Tracker() override { flush_(true); }

protected:
    void on_message(const std::string &, const std::shared_ptr<Event> &event) override;
    std::string transaction_name_;

private:
    std::unordered_set<std::shared_ptr<Transaction>> inflight_transactions;
    // normally we don't flush finished transactions to disk unless relevant functions
    // are called, since transaction data are not that big.
    TransactionBatch finished_transactions_;

    // avoid virtual function lookup during construction
    void flush_(bool save_inflight_transaction);
};

class GroupTracker: public TrackerBase {
public:
    explicit GroupTracker(const std::string &topic): TrackerBase(topic) {}
    GroupTracker(MessageBus *bus, std::string topic): TrackerBase(bus, std::move(topic)) {}
};

extern void add_tracker_to_simulator(const std::shared_ptr<Tracker> &tracker);

}  // namespace hermes

#endif  // HERMES_TRACKER_HH
