#ifndef HERMES_LOADER_HH
#define HERMES_LOADER_HH

#include "transaction.hh"

namespace arrow::fs {
class FileSystem;
}

namespace hermes {

struct FileInfo {
public:
    enum class FileType { event, transaction };
    FileInfo(FileType type, std::string filename, uint64_t min_id, uint64_t max_id,
             uint64_t min_time, uint64_t max_time)
        : type(type),
          filename(std::move(filename)),
          min_id(min_id),
          max_id(max_id),
          min_time(min_time),
          max_time(max_time) {}
    FileType type;
    std::string filename;
    uint64_t min_id;
    uint64_t max_id;
    uint64_t min_time;
    uint64_t max_time;

    std::string name;
};

struct LoaderResult {
    std::shared_ptr<arrow::Table> table;
    std::string name;
};

struct TransactionData {
    std::shared_ptr<Transaction> transaction;
    std::shared_ptr<EventBatch> events;
};

class Loader;
struct TransactionDataIter {
public:
    TransactionDataIter(Loader *loader, TransactionBatch::iterator it) : loader_(loader), it_(it) {}
    TransactionData operator*() const;

    inline TransactionDataIter &operator++() {
        it_++;
        return *this;
    }

    inline TransactionDataIter operator++(int) {  // NOLINT
        return TransactionDataIter(loader_, it_ + 1);
    }

    friend bool operator==(const TransactionDataIter &a, const TransactionDataIter &b) {
        return a.it_ == b.it_;
    }

    friend bool operator!=(const TransactionDataIter &a, const TransactionDataIter &b) {
        return a.it_ != b.it_;
    }

    inline TransactionDataIter operator+(uint64_t index) {
        return TransactionDataIter(loader_, it_ + index);
    }

private:
    Loader *loader_;
    TransactionBatch::iterator it_;
};

class TransactionStream {
public:
    TransactionStream(std::shared_ptr<TransactionBatch> transactions, Loader *loader)
        : transactions_(std::move(transactions)), loader_(loader) {}

    [[nodiscard]] inline TransactionDataIter begin() const {
        return TransactionDataIter(loader_, transactions_->begin());
    }
    [[nodiscard]] inline TransactionDataIter end() const {
        return TransactionDataIter(loader_, transactions_->end());
    }

    [[nodiscard]] uint64_t size() const { return transactions_->size(); }

private:
    std::shared_ptr<TransactionBatch> transactions_;
    Loader *loader_;
};

class Loader {
public:
    explicit Loader(std::string dir);
    std::vector<LoaderResult> get_transactions(uint64_t min_time, uint64_t max_time);

    std::vector<LoaderResult> get_events(uint64_t min_time, uint64_t max_time);

    std::vector<LoaderResult> get_transactions() {
        return get_transactions(0, std::numeric_limits<uint64_t>::max());
    }

    std::shared_ptr<TransactionStream> get_transaction_stream(const std::string &name);

    EventBatch get_events(const Transaction &transaction);

    // debug information
    [[maybe_unused]] void print_files() const;

private:
    std::string dir_;
    std::vector<std::unique_ptr<FileInfo>> files_;
    std::shared_ptr<arrow::fs::FileSystem> fs_;
    // indices
    std::vector<const FileInfo *> events_;
    std::vector<const FileInfo *> transactions_;
    std::unordered_map<const FileInfo *, std::shared_ptr<arrow::Table>> tables_;
    // local caches
    std::unordered_map<const arrow::Table *, std::unique_ptr<EventBatch>> event_cache_;

    void load_json(const std::string &path);
    std::shared_ptr<arrow::Table> load_table(const FileInfo *info);
    std::vector<LoaderResult> load_tables(const std::vector<const FileInfo *> &files);
    EventBatch *load_events(const std::shared_ptr<arrow::Table> &table);
};

}  // namespace hermes

#endif  // HERMES_LOADER_HH
