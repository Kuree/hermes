#ifndef HERMES_TRANSACTION_HH
#define HERMES_TRANSACTION_HH

#include "event.hh"

namespace hermes {

class TransactionBatch;

class Transaction {
public:
    explicit Transaction(uint64_t id) noexcept : id_(id) {}
    bool add_event(const std::unique_ptr<Event> &event);
    void finish() { is_done_ = true; }
    [[nodiscard]] bool finished() const { return is_done_; }
    [[nodiscard]] uint64_t id() const { return id_; }
    [[nodiscard]] const std::vector<uint64_t> &events() const { return events_ids_; }
    [[nodiscard]] uint64_t start_time() const { return start_time_; }
    [[nodiscard]] uint64_t end_time() const { return end_time_; }

private:
    uint64_t id_;
    uint64_t start_time_ = std::numeric_limits<uint64_t>::max();
    uint64_t end_time_ = 0;
    bool is_done_ = false;
    std::vector<uint64_t> events_ids_;

    friend TransactionBatch;
};

class TransactionBatch : public std::vector<std::unique_ptr<Transaction>> {
public:
    std::pair<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::Schema>> serialize()
        const noexcept;
    // factory method to construct transaction batch
    static std::unique_ptr<TransactionBatch> deserialize(
        const std::shared_ptr<arrow::Buffer> &buffer);
};

}  // namespace hermes

#endif  // HERMES_TRANSACTION_HH
