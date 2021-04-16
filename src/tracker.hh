#ifndef HERMES_TRACKER_HH
#define HERMES_TRACKER_HH

#include <unordered_set>

#include "pubsub.hh"
#include "serializer.hh"
#include "transaction.hh"

namespace hermes {

template <typename TargetObject, typename TransactionObject, typename BatchType>
class TrackerBase : public Subscriber {
public:
    explicit TrackerBase(const std::string &topic)
        : TrackerBase(MessageBus::default_bus(), topic) {}
    TrackerBase(MessageBus *bus, std::string topic) : topic_(std::move(topic)) { bus_ = bus; }
    void connect() { subscribe(bus_, topic_); }

    void set_serializer(const std::shared_ptr<Serializer> &serializer) { serializer_ = serializer; }

    void flush(bool save_inflight_transaction = true) {
        if (!serializer_) return;
        if (!finished_transactions_.empty()) {
            serializer_->serialize(finished_transactions_);
            finished_transactions_.clear();
        }

        if (save_inflight_transaction) {
            if (!inflight_transactions.empty()) {
                // we reuse the finished transaction so that it will be saved in the
                // same places
                finished_transactions_.reserve(inflight_transactions.size());
                for (auto const &transaction : inflight_transactions) {
                    finished_transactions_.emplace_back(transaction);
                }
                serializer_->serialize(finished_transactions_);
                inflight_transactions.clear();
                finished_transactions_.clear();
            }
        }
    }

    TransactionObject *get_new_transaction() {
        // we use the default id allocator
        auto t = std::make_shared<TransactionObject>();
        auto *ptr = t.get();
        inflight_transactions.emplace(t);
        t->set_name(transaction_name_);
        return ptr;
    }

    [[maybe_unused]] void set_transaction_name(std::string transaction_name) {
        transaction_name_ = std::move(transaction_name);
        finished_transactions_.set_name(transaction_name_);
    }
    
    const std::string &transaction_name() const { return transaction_name_; }
    [[nodiscard]] const BatchType &finished_transactions() const { return finished_transactions_; }
    [[nodiscard]] bool publish_transaction() const { return publish_transaction_; }
    void set_publish_transaction(bool value) { publish_transaction_ = value; }

    virtual TransactionObject *track(TargetObject *event) = 0;

    void retire_transaction(const std::shared_ptr<TransactionObject> &transaction) {
        inflight_transactions.erase(transaction);
        finished_transactions_.emplace_back(transaction);

        // decide whether to flush
        if (finished_transactions_.size() >= transaction_flush_threshold_) {
            flush(false);
        }

        if (publish_transaction_) {
            bus_->publish(transaction_name_, transaction);
        }
    }

    ~TrackerBase() { flush(true); }
    void stop() override { flush(true); }

protected:
    std::string transaction_name_;
    // how often to flush the transactions to files
    constexpr static uint64_t transaction_flush_threshold_ = 1 << 16;
    // serializer
    std::shared_ptr<Serializer> serializer_;

private:
    std::string topic_;

    std::unordered_set<std::shared_ptr<TransactionObject>> inflight_transactions;
    // normally we don't flush finished transactions to disk unless relevant functions
    // are called, since transaction data are not that big.
    BatchType finished_transactions_;

    // whether to publish the finished transactions
    bool publish_transaction_ = false;
};

class Tracker : public TrackerBase<Event, Transaction, TransactionBatch> {
public:
    explicit Tracker(const std::string &topic) : TrackerBase(topic) {}
    Tracker(MessageBus *bus, std::string topic) : TrackerBase(bus, std::move(topic)) {}

protected:
    void on_message(const std::string &, const std::shared_ptr<Event> &event) override;
};

class GroupTracker : public TrackerBase<Transaction, TransactionGroup, TransactionGroupBatch> {
public:
    explicit GroupTracker(const std::string &topic) : TrackerBase(topic) {}
    GroupTracker(MessageBus *bus, std::string topic) : TrackerBase(bus, std::move(topic)) {}

protected:
    void on_message(const std::string &, const std::shared_ptr<Transaction> &transaction) override;
};

extern void add_tracker_to_simulator(const std::shared_ptr<Tracker> &tracker);

}  // namespace hermes

#endif  // HERMES_TRACKER_HH
