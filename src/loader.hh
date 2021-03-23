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

    uint64_t usage_count = 0;
};

class Loader {
public:
    explicit Loader(std::string dir);
    std::vector<std::shared_ptr<arrow::Table>> get_transactions(uint64_t min_time,
                                                                uint64_t max_time);

    std::vector<std::shared_ptr<arrow::Table>> get_events(uint64_t min_time, uint64_t max_time);

private:
    std::string dir_;
    std::vector<std::unique_ptr<FileInfo>> files_;
    std::shared_ptr<arrow::fs::FileSystem> fs_;
    // indices
    std::vector<const FileInfo *> events_;
    std::vector<const FileInfo *> transactions_;
    // local caches
    std::unordered_map<const FileInfo *, std::shared_ptr<arrow::Table>> tables_;

    void load_json(const std::string &path);
    std::shared_ptr<arrow::Table> load_table(const std::string &filename);
    std::vector<std::shared_ptr<arrow::Table>> load_tables(
        const std::vector<const FileInfo *> &files);
};

}  // namespace hermes

#endif  // HERMES_LOADER_HH
