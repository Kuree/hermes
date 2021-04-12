#ifndef HERMES_TRANSACTION_HH
#define HERMES_TRANSACTION_HH

#include "event.hh"

namespace hermes {

class TransactionBatch;

class Transaction : public std::enable_shared_from_this<Transaction> {
public:
    Transaction() noexcept;
    explicit Transaction(uint64_t id) noexcept : id_(id) {}
    bool add_event(const std::shared_ptr<Event> &event) { return add_event(event.get()); }
    bool add_event(const Event *event);
    void finish() { finished_ = true; }
    [[nodiscard]] bool finished() const { return finished_; }
    [[nodiscard]] uint64_t id() const { return id_; }
    [[nodiscard]] const std::vector<uint64_t> &events() const { return events_ids_; }
    [[nodiscard]] uint64_t start_time() const { return start_time_; }
    [[nodiscard]] uint64_t end_time() const { return end_time_; }

private:
    uint64_t id_;
    uint64_t start_time_ = std::numeric_limits<uint64_t>::max();
    uint64_t end_time_ = 0;
    bool finished_ = false;
    std::vector<uint64_t> events_ids_;

    static uint64_t id_allocator_;

    friend TransactionBatch;
};

class TransactionBatch : public Batch<Transaction, TransactionBatch> {
public:
    [[nodiscard]] std::pair<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::Schema>>
    serialize() const noexcept override;
    // factory method to construct transaction batch
    static std::unique_ptr<TransactionBatch> deserialize(
        const std::shared_ptr<arrow::Table> &table);

    void set_transaction_name(std::string name) { transaction_name_ = std::move(name); }
    [[nodiscard]] const std::string &transaction_name() const { return transaction_name_; }

    TransactionBatch::iterator lower_bound(uint64_t time);

    // we sort based on the finishing time (end time)
    void sort() override;

    bool contains(uint64_t id) override;

private:
    std::string transaction_name_;

    std::unordered_map<uint64_t, Transaction*> id_index_;
    std::map<uint64_t, TransactionBatch::iterator> time_lower_bound_;

    void build_time_index();
    void build_id_index();
};

}  // namespace hermes

#endif  // HERMES_TRANSACTION_HH
