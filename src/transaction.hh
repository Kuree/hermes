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

class TransactionBatch : public std::vector<std::shared_ptr<Transaction>> {
public:
    [[nodiscard]] std::pair<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::Schema>>
    serialize() const noexcept;
    // factory method to construct transaction batch
    static std::unique_ptr<TransactionBatch> deserialize(
        const std::shared_ptr<arrow::Table> &table);
    static std::unique_ptr<TransactionBatch> deserialize(const std::shared_ptr<arrow::Table> &table,
                                                         const std::vector<uint64_t> &row_groups);

    void set_transaction_name(std::string name) { transaction_name_ = std::move(name); }
    [[nodiscard]] const std::string &transaction_name() const { return transaction_name_; }

private:
    std::string transaction_name_;
};

}  // namespace hermes

#endif  // HERMES_TRANSACTION_HH
