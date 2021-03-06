#ifndef HERMES_LOADER_HH
#define HERMES_LOADER_HH

#include <mutex>
#include <set>

#include "arrow.hh"
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
    uint64_t size = 0;

    std::string name;

    static std::string type_str(FileType type);
};

struct LoaderResult {
    const arrow::Table *table;
    std::string name;
};

struct FileMetadata {
public:
    using Value = std::map<std::string, std::vector<std::shared_ptr<parquet::Statistics>>>;
    FileMetadata() = default;
    void emplace(const FileInfo *key, const Value &value) { data_.emplace(key, value); }

    Value &operator[](const FileInfo *key) { return data_[key]; }

    [[nodiscard]] const Value &at(const FileInfo *info) const { return data_.at(info); }

    uint64_t size(const FileInfo *file) { return data_.at(file).begin()->second.size(); }

private:
    std::unordered_map<const FileInfo *,
                       std::map<std::string, std::vector<std::shared_ptr<parquet::Statistics>>>>
        data_;
};

struct TransactionData {
public:
    struct TransactionGroupData {
        std::shared_ptr<TransactionGroup> group;
        std::vector<TransactionData> values;
    };

    std::shared_ptr<Transaction> transaction;
    std::shared_ptr<EventBatch> events;
    std::optional<TransactionGroupData> group;

    template <std::size_t N>
    decltype(auto) get() const {
        if constexpr (N == 0)
            return transaction;
        else if constexpr (N == 1)
            return events;
        else if constexpr (N == 2)
            return group;
    }

    [[nodiscard]] bool is_group() const { return group.has_value(); }
};

class TransactionStream;
struct TransactionDataIter {
public:
    TransactionDataIter(const TransactionStream *stream, uint64_t current_row);
    TransactionData operator*() const;

    inline TransactionDataIter &operator++() {
        current_row_++;
        compute_index();
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
    const TransactionStream *stream_;
    uint64_t current_row_ = 0;

    uint64_t table_index_;
    std::pair<bool, const arrow::Table *> table_entry_;

    void compute_index();
};

class Loader;
class TransactionStream {
public:
    // <is_group, table>
    TransactionStream(const std::vector<std::pair<bool, const arrow::Table *>> &tables,
                      Loader *loader);

    [[nodiscard]] inline TransactionDataIter begin() const { return TransactionDataIter(this, 0); }
    [[nodiscard]] inline TransactionDataIter end() const {
        return TransactionDataIter(this, num_entries_);
    }

    [[nodiscard]] uint64_t size() const { return num_entries_; }

    TransactionStream where(const std::function<bool(const TransactionData &data)> &filter) const;

    [[nodiscard]] std::string json() const;

private:
    std::map<uint64_t, std::pair<bool, const arrow::Table *>> tables_;
    uint64_t num_entries_ = 0;
    Loader *loader_ = nullptr;

    // row mapping, used for filtering
    std::optional<std::vector<std::vector<uint64_t>>> row_mapping_;

    TransactionStream(const std::vector<std::pair<bool, const arrow::Table *>> &tables,
                      Loader *loader, std::vector<std::vector<uint64_t>> row_mapping);

    TransactionStream() = default;

    friend class TransactionDataIter;
};

struct LoaderStats {
    uint64_t min_event_time = std::numeric_limits<uint64_t>::max();
    uint64_t max_event_time = 0;
    uint64_t num_events = 0;
    uint64_t num_transactions = 0;
    uint64_t num_transaction_groups = 0;
    uint64_t num_event_files = 0;
    uint64_t num_transaction_files = 0;
    uint64_t num_transaction_group_files = 0;
    uint64_t average_event_chunk_size = 0;
    uint64_t average_transaction_chunk_size = 0;
    uint64_t average_transaction_group_chunk_size = 0;
};

// define table schema so that some downstream tools can directly
// interact with the raw parquet files
enum class EventDataType { bool_, uint8_t_, uint16_t_, uint32_t_, uint64_t_, string };

using BatchSchema = std::map<std::string, EventDataType>;

class MessageBus;
class Checker;
class Loader {
public:
    explicit Loader(const std::string &dir);
    explicit Loader(const std::vector<std::string> &dirs);
    explicit Loader(const std::vector<FileSystemInfo> &infos);
    std::shared_ptr<Transaction> get_transaction(uint64_t id);
    std::shared_ptr<TransactionGroup> get_transaction_group(uint64_t id);
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
    std::shared_ptr<TransactionStream> get_transaction_stream(const std::string &name,
                                                              uint64_t start_time,
                                                              uint64_t end_time);

    std::shared_ptr<EventBatch> get_events(const Transaction &transaction);

    [[nodiscard]] BatchSchema get_event_schema(const std::string &name);

    [[nodiscard]] std::set<std::string> get_event_names() const;
    [[nodiscard]] std::set<std::string> get_transaction_names() const;
    [[nodiscard]] std::set<std::string> get_transaction_group_names() const;

    // access to raw data
    [[nodiscard]] const std::map<std::pair<const FileInfo *, uint64_t>,
                                 std::shared_ptr<arrow::Table>>
        &tables() const {
        return tables_;
    }

    void stream(bool stream_transactions = true);
    void stream(MessageBus *bus, bool stream_transactions = true);

    // debug information
    [[maybe_unused]] void print_files() const;
    void preload();

private:
    std::mutex files_mutex_;
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
    // lookup table for event id lists
    // this will be read-only so no mutex protection
    std::map<uint64_t, std::pair<const FileInfo *, uint64_t>> event_id_index_;
    // only if we have preloaded everything
    bool preloaded_ = false;

    void open_dir(const FileSystemInfo &info);
    void load_json(const std::string &json_info, const std::shared_ptr<arrow::fs::FileSystem> &fs);
    bool preload_table(const FileInfo *info,
                       const std::shared_ptr<arrow::io::RandomAccessFile> &file);
    std::vector<LoaderResult> load_tables(
        const std::vector<std::pair<const FileInfo *, std::vector<uint64_t>>> &files);
    std::shared_ptr<EventBatch> load_events(const arrow::Table *table);
    std::shared_ptr<TransactionBatch> load_transactions(const arrow::Table *table);
    std::shared_ptr<TransactionGroupBatch> load_transaction_groups(const arrow::Table *table);
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
    bool contains_time(const FileInfo *file, uint64_t chunk_id, uint64_t min_time,
                       uint64_t max_time);
    void init_cache();
    void compute_event_id_index();

    static uint64_t compute_table_size_in_memory(const std::shared_ptr<arrow::Table> &table);
    static std::vector<std::string> load_checkpoint_info(
        const std::shared_ptr<arrow::io::RandomAccessFile> &file);

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
