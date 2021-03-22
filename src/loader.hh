#ifndef HERMES_LOADER_HH
#define HERMES_LOADER_HH

#include "transaction.hh"

namespace hermes {

struct FileInfo {
public:
    FileInfo(std::string filename, uint64_t min_id, uint64_t max_id, uint64_t min_time,
             uint64_t max_time)
        : filename(std::move(filename)),
          min_id(min_id),
          max_id(max_id),
          min_time(min_time),
          max_time(max_time) {}
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

private:
    std::string dir_;
    std::vector<std::unique_ptr<FileInfo>> files_;
    // indices
    std::vector<const FileInfo *> events_;
    std::vector<const FileInfo *> transactions_;
    // local caches
    std::unordered_map<const FileInfo *, std::shared_ptr<arrow::Table>> tables_;

    void load_json(const std::string &path);
    static std::shared_ptr<arrow::Table> load_table(const std::string &filename);
};

}  // namespace hermes

#endif  // HERMES_LOADER_HH
