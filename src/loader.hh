#ifndef HERMES_LOADER_HH
#define HERMES_LOADER_HH

#include <mutex>

#include "cache.hh"
#include "transaction.hh"

namespace arrow {
namespace fs {
class FileSystem;
class FileInfo;
}  // namespace fs
namespace io {
class RandomAccessFile;
}
}  // namespace arrow

namespace parquet {
class Statistics;
}

namespace hermes {

struct FileInfo {
public:
    enum class FileType { event, transaction, transaction_group };
    FileInfo(FileType type, std::string filename) : type(type), filename(std::move(filename)) {}
    FileType type;
    std::string filename;

    std::string name;

    static std::string type_str(FileType type);
};

struct LoaderResult {
    std::shared_ptr<arrow::Table> table;
    std::string name;
};

struct TransactionData {
public:
    std::shared_ptr<Transaction> transaction;
    std::shared_ptr<EventBatch> events;

    template <std::size_t N>
    decltype(auto) get() const {
        if constexpr (N == 0)
            return transaction;
        else if constexpr (N == 1)
            return events;
    }
};

class TransactionStream;
struct TransactionDataIter {
public:
    TransactionDataIter(TransactionStream *stream, uint64_t current_row)
        : stream_(stream), current_row_(current_row) {}
    TransactionData operator*() const;

    inline TransactionDataIter &operator++() {
        current_row_++;
        return *this;
    }

    inline TransactionDataIter operator++(int) {  // NOLINT
        return TransactionDataIter(stream_, current_row_ + 1);
    }

    friend bool operator==(const TransactionDataIter &a, const TransactionDataIter &b) {
        return a.current_row_ == b.current_row_;
    }

    friend bool operator!=(const TransactionDataIter &a, const TransactionDataIter &b) {
        return a.current_row_ != b.current_row_;
    }

    inline TransactionDataIter operator+(uint32_t index) {
        return TransactionDataIter(stream_, current_row_ + index);
    }

private:
    TransactionStream *stream_;
    uint64_t current_row_ = 0;
};

class Loader;
class TransactionStream {
public:
    TransactionStream(const std::vector<std::shared_ptr<arrow::Table>> &tables, Loader *loader);

    [[nodiscard]] inline TransactionDataIter begin() { return TransactionDataIter(this, 0); }
    [[nodiscard]] inline TransactionDataIter end() {
        return TransactionDataIter(this, num_entries_);
    }

    [[nodiscard]] uint64_t size() const { return num_entries_; }

private:
    std::map<uint64_t, std::shared_ptr<arrow::Table>> tables_;
    uint64_t num_entries_ = 0;
    Loader *loader_;

    friend class TransactionDataIter;
};

struct LoaderStats {
    uint64_t min_event_time = std::numeric_limits<uint64_t>::max();
    uint64_t max_event_time = 0;
    uint64_t num_events = 0;
    uint64_t num_transactions = 0;
    uint64_t num_event_files = 0;
    uint64_t num_transaction_files = 0;
    uint64_t average_event_chunk_size = 0;
    uint64_t average_transaction_chunk_size = 0;
};

class MessageBus;
class Checker;
class Loader {
public:
    explicit Loader(const std::string &dir);
    explicit Loader(const std::vector<std::string> &dirs);
    std::shared_ptr<Transaction> get_transaction(uint64_t id);
    std::vector<std::shared_ptr<TransactionBatch>> get_transactions(uint64_t min_time,
                                                                    uint64_t max_time);
    std::shared_ptr<TransactionBatch> get_transactions(const std::string &name, uint64_t min_time,
                                                       uint64_t max_time);
    std::shared_ptr<TransactionGroupBatch> get_transaction_groups(const std::string &name,
                                                                  uint64_t min_time,
                                                                  uint64_t max_time);

    std::shared_ptr<TransactionBatch> get_transactions(
        const std::shared_ptr<Transaction> &transaction);

    std::vector<std::shared_ptr<EventBatch>> get_events(uint64_t min_time, uint64_t max_time);
    std::shared_ptr<EventBatch> get_events(const std::string &name, uint64_t min_time,
                                           uint64_t max_time);

    std::shared_ptr<TransactionStream> get_transaction_stream(const std::string &name);

    std::shared_ptr<EventBatch> get_events(const Transaction &transaction);

    void stream(bool stream_transactions = true);
    void stream(MessageBus *bus, bool stream_transactions = true);

    // debug information
    [[maybe_unused]] void print_files() const;

    using FileMetadata = std::unordered_map<
        const FileInfo *, std::map<std::string, std::vector<std::shared_ptr<parquet::Statistics>>>>;

private:
    std::vector<std::unique_ptr<FileInfo>> files_;
    // indices
    std::vector<const FileInfo *> events_;
    std::vector<const FileInfo *> transactions_;
    std::vector<const FileInfo *> transaction_groups_;
    std::map<std::pair<const FileInfo *, uint64_t>, std::shared_ptr<arrow::Table>> tables_;
    // we store all the statistics here
    FileMetadata file_metadata_;
    // local caches
    std::mutex event_cache_mutex_;
    std::unique_ptr<lru_cache<const arrow::Table *, std::shared_ptr<EventBatch>>> event_cache_;
    std::mutex transaction_cache_mutex_;
    std::unique_ptr<lru_cache<const arrow::Table *, std::shared_ptr<TransactionBatch>>>
        transaction_cache_;
    std::mutex transaction_group_cache_mutex_;
    std::unique_ptr<lru_cache<const arrow::Table *, std::shared_ptr<TransactionGroupBatch>>>
        transaction_group_cache_;
    // stats about the folder we're reading
    LoaderStats stats_;

    void open_dir(const std::string &dir);
    void load_json(const arrow::fs::FileInfo &json_info,
                   const std::shared_ptr<arrow::fs::FileSystem> &fs);
    bool preload_table(const FileInfo *info,
                       const std::shared_ptr<arrow::io::RandomAccessFile> &file);
    std::vector<LoaderResult> load_tables(
        const std::vector<std::pair<const FileInfo *, std::vector<uint64_t>>> &files);
    std::shared_ptr<EventBatch> load_events(const std::shared_ptr<arrow::Table> &table);
    std::shared_ptr<TransactionBatch> load_transactions(const std::shared_ptr<arrow::Table> &table);
    std::shared_ptr<TransactionGroupBatch> load_transaction_groups(
        const std::shared_ptr<arrow::Table> &table);
    void compute_stats();

    // only return the table
    std::vector<LoaderResult> load_events_table(uint64_t min_time, uint64_t max_time);
    std::vector<LoaderResult> load_transaction_table(const std::optional<std::string> &name,
                                                     uint64_t min_time, uint64_t max_time);
    std::vector<LoaderResult> load_transaction_group_table(const std::optional<std::string> &name,
                                                           uint64_t min_time, uint64_t max_time);
    std::vector<LoaderResult> load_batch_table(const std::vector<const FileInfo *> &info,
                                               const std::optional<std::string> &name,
                                               uint64_t min_time, uint64_t max_time);

    friend class Checker;
    friend class TransactionDataIter;
};

}  // namespace hermes

namespace std {
template <std::size_t N>
struct tuple_element<N, hermes::TransactionData> {
    using type = decltype(std::declval<hermes::TransactionData>().get<N>());
};
}  // namespace std

#endif  // HERMES_LOADER_HH
