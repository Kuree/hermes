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
};

class Loader {
public:
    explicit Loader(std::string dir);

private:
    std::string dir_;
    std::vector<std::unique_ptr<FileInfo>> files_;
    // indices
    std::unordered_map<std::string, FileInfo *> filename_info_;
    std::vector<const FileInfo *> events_;
    std::vector<const FileInfo *> transactions_;

    void load_json(const std::string &path);
};

}  // namespace hermes

#endif  // HERMES_LOADER_HH
