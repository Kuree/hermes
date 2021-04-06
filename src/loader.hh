#ifndef HERMES_LOADER_HH
#define HERMES_LOADER_HH

#include "transaction.hh"

namespace arrow::fs {
class FileSystem;
}

namespace parquet {
class Statistics;
}

namespace hermes {

struct FileInfo {
public:
    enum class FileType { event, transaction };
    FileInfo(FileType type, std::string filename) : type(type), filename(std::move(filename)) {}
    FileType type;
    std::string filename;

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
    std::vector<std::unique_ptr<TransactionBatch>> get_transactions(uint64_t min_time,
                                                                    uint64_t max_time);

    std::vector<std::unique_ptr<EventBatch>> get_events(uint64_t min_time, uint64_t max_time);
    std::unique_ptr<EventBatch> get_events(const std::string &name, uint64_t min_time,
                                           uint64_t max_time);

    std::shared_ptr<TransactionStream> get_transaction_stream(const std::string &name);

    EventBatch get_events(const Transaction &transaction);

    // debug information
    [[maybe_unused]] void print_files() const;

    using FileMetadata = std::unordered_map<const FileInfo *,
        std::map<std::string, std::vector<std::shared_ptr<parquet::Statistics>>>>;

private:
    std::string dir_;
    std::vector<std::unique_ptr<FileInfo>> files_;
    std::shared_ptr<arrow::fs::FileSystem> fs_;
    // indices
    std::vector<const FileInfo *> events_;
    std::vector<const FileInfo *> transactions_;
    std::map<std::pair<const FileInfo *, uint64_t>, std::shared_ptr<arrow::Table>> tables_;
    // we store all the statistics here
    FileMetadata file_metadata_;
    // local caches
    std::unordered_map<const arrow::Table *, std::shared_ptr<EventBatch>> event_cache_;

    void load_json(const std::string &path);
    bool preload_table(const FileInfo *info);
    std::vector<LoaderResult> load_tables(
        const std::vector<std::pair<const FileInfo *, std::vector<uint64_t>>> &files);
    EventBatch *load_events(const std::shared_ptr<arrow::Table> &table);
};

}  // namespace hermes

#endif  // HERMES_LOADER_HH
