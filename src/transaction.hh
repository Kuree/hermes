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
    void finish();
    [[nodiscard]] bool finished() const { return finished_; }
    [[nodiscard]] uint64_t id() const { return id_; }
    [[nodiscard]] const std::vector<uint64_t> &events() const { return events_ids_; }
    [[nodiscard]] uint64_t start_time() const { return start_time_; }
    [[nodiscard]] uint64_t end_time() const { return end_time_; }
    [[nodiscard]] const std::string &name() const { return name_; }
    void set_name(const std::string &name) { name_ = name; }
    void set_on_finished(const std::function<void(Transaction *)> &func) { on_finished_ = func; }

    void static reset_id() { id_allocator_ = 0; }

private:
    uint64_t id_;
    uint64_t start_time_ = std::numeric_limits<uint64_t>::max();
    uint64_t end_time_ = 0;
    std::string name_;
    bool finished_ = false;
    std::vector<uint64_t> events_ids_;

    static std::atomic<uint64_t> id_allocator_;

    // callback for trackers
    std::optional<std::function<void(Transaction *)>> on_finished_;

    friend TransactionBatch;
};

class TransactionBatch : public Batch<Transaction, TransactionBatch> {
public:
    [[nodiscard]] std::pair<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::Schema>>
    serialize() const noexcept override;
    // factory method to construct transaction batch
    static std::unique_ptr<TransactionBatch> deserialize(const arrow::Table *table);

    TransactionBatch::iterator lower_bound(uint64_t time);

    // we sort based on the finishing time (end time)
    void sort() override;

    bool contains(uint64_t id) override;
    std::shared_ptr<Transaction> at(uint64_t id) const;

    void build_time_index();
    void build_id_index();

private:
    std::unordered_map<uint64_t, Transaction *> id_index_;
    std::map<uint64_t, TransactionBatch::iterator> time_lower_bound_;
};

class TransactionGroupBatch;
class TransactionGroup : public std::enable_shared_from_this<TransactionGroup> {
    // tree based structure, also optimized for parquet
public:
    explicit TransactionGroup(uint64_t id);
    TransactionGroup();

    void add_transaction(const std::shared_ptr<Transaction> &transaction);
    void add_transaction(const std::shared_ptr<TransactionGroup> &group);

    [[nodiscard]] uint64_t id() const { return id_; }
    [[nodiscard]] const std::vector<uint64_t> &transactions() const { return transactions_; }
    [[nodiscard]] const std::vector<bool> &transaction_masks() const { return transaction_masks_; }
    [[nodiscard]] auto start_time() const { return start_time_; }
    [[nodiscard]] auto end_time() const { return end_time_; }

    [[nodiscard]] auto size() const { return transactions_.size(); }
    [[nodiscard]] auto const &name() const { return name_; }

    void set_name(const std::string &name) { name_ = name; }
    void finish();
    [[nodiscard]] auto finished() const { return finished_; }
    void set_on_finished(const std::function<void(TransactionGroup *)> &func) {
        on_finished_ = func;
    }

    void static reset_id() { id_allocator_ = 0; }

private:
    uint64_t id_;
    std::vector<uint64_t> transactions_;
    // true means group, false means the actual transaction
    std::vector<bool> transaction_masks_;
    uint64_t start_time_ = std::numeric_limits<uint64_t>::max();
    uint64_t end_time_ = 0;
    std::string name_;
    bool finished_ = false;

    // callback for trackers
    std::optional<std::function<void(TransactionGroup *)>> on_finished_;

    static std::atomic<uint64_t> id_allocator_;

    friend class TransactionGroupBatch;
};

class TransactionGroupBatch : public Batch<TransactionGroup, TransactionGroupBatch> {
public:
    [[nodiscard]] std::pair<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::Schema>>
    serialize() const noexcept override;
    // factory method to construct transaction group batch
    static std::unique_ptr<TransactionGroupBatch> deserialize(const arrow::Table *table);

    void sort() override;

    bool contains(uint64_t id) override;
    std::shared_ptr<TransactionGroup> at(uint64_t id);

    void build_index();

private:
    std::unordered_map<uint64_t, TransactionGroup *> id_index_;
};

}  // namespace hermes

#endif  // HERMES_TRANSACTION_HH
