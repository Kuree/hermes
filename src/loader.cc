#include "loader.hh"

#include <filesystem>
#include <fstream>

#include "arrow/api.h"
#include "arrow/filesystem/localfs.h"
#include "fmt/format.h"
#include "parquet/arrow/reader.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace fs = std::filesystem;

namespace hermes {

Loader::Loader(std::string dir) : dir_(fs::absolute(std::move(dir))) {
    // do a LIST operation
    for (auto const &entry : fs::directory_iterator(dir_)) {
        auto path = fs::path(entry);
        if (!fs::is_regular_file(path)) {
            // we ara only interested in regular file
            continue;
        }
        auto ext = path.extension();
        if (ext == ".json") {
            // json file, parse it and load it up
            load_json(path);
        }
    }
    fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
}

std::vector<std::shared_ptr<arrow::Table>> Loader::get_transactions(uint64_t min_time,
                                                                    uint64_t max_time) {
    // need to go through each
    // maybe index on the time?
    // I don't expect the number of files to be bigger than 1000 (assume each file is about 10MB)
    // linear scan should be sufficient
    std::vector<const FileInfo *> files;
    files.reserve(16);
    for (auto const &file : transactions_) {
        if (file->max_time < min_time || file->min_time > max_time) continue;
        files.emplace_back(file);
    }
    // load the tables
    return load_tables(files);
}

std::vector<std::shared_ptr<arrow::Table>> Loader::get_events(uint64_t min_time,
                                                              uint64_t max_time) {
    std::vector<const FileInfo *> files;
    files.reserve(16);
    for (auto const &file : events_) {
        if (file->max_time < min_time || file->min_time > max_time) continue;
        files.emplace_back(file);
    }
    // load the tables
    return load_tables(files);
}

static bool check_member(rapidjson::Document &document, const char *member_name) {
    return document.HasMember(member_name);
}

template <typename T>
static std::optional<T> get_member(rapidjson::Document &document, const char *member_name) {
    if (!check_member(document, member_name)) return std::nullopt;
    if constexpr (std::is_same<T, std::string>::value) {
        if (document[member_name].IsString()) {
            return std::string(document[member_name].GetString());
        }
    } else if constexpr (std::is_integral<T>::value && !std::is_same<T, bool>::value) {
        if (document[member_name].IsNumber()) {
            return document[member_name].template Get<T>();
        }
    } else if constexpr (std::is_same<T, bool>::value) {
        if (document[member_name].IsBool()) {
            return document[member_name].GetBool();
        }
    }
    return std::nullopt;
}

void Loader::load_json(const std::string &path) {
    std::ifstream stream(path);
    if (stream.bad()) return;
    // https://stackoverflow.com/a/2602258
    stream.seekg(0, std::ios::end);
    auto size = stream.tellg();
    std::string content(size, ' ');
    stream.seekg(0);
    stream.read(&content[0], size);

    rapidjson::Document document;
    document.Parse(content.c_str());
    if (document.HasParseError()) return;
    // get indexed value
    auto opt_parquet_file = get_member<std::string>(document, "parquet");
    if (!opt_parquet_file) return;
    auto parquet_file = *opt_parquet_file;

    auto opt_type = get_member<std::string>(document, "type");
    if (!opt_type) return;
    auto const &type = *opt_type;

    std::pair<uint64_t, uint64_t> ids, times;
    const static std::vector minmax = {"min", "max"};
    for (auto i = 0; i < 2; i++) {
        auto name = fmt::format("{0}_id", minmax[i]);
        auto value = get_member<uint64_t>(document, name.c_str());
        if (!value) return;
        if (i == 0)
            ids.first = *value;
        else
            ids.second = *value;
        name = fmt::format("{0}_time", minmax[i]);
        value = get_member<uint64_t>(document, name.c_str());
        if (!value) return;
        if (i == 0)
            times.first = *value;
        else
            times.second = *value;
    }

    parquet_file = fs::path(path).parent_path() / parquet_file;
    auto info = std::make_unique<FileInfo>(
        type == "event" ? FileInfo::FileType::event : FileInfo::FileType::transaction, parquet_file,
        ids.first, ids.second, times.first, times.second);
    if (type == "event") {
        events_.emplace_back(info.get());
    } else if (type == "transaction") {
        transactions_.emplace_back(info.get());
    } else {
        return;
    }
    files_.emplace_back(std::move(info));
}

std::shared_ptr<arrow::Table> Loader::load_table(const std::string &filename) {
    auto res_f = fs_->OpenInputFile(filename);
    if (!res_f.ok()) return nullptr;
    auto f = *res_f;
    auto *pool = arrow::default_memory_pool();
    std::unique_ptr<parquet::arrow::FileReader> file_reader;
    auto res = parquet::arrow::OpenFile(f, pool, &file_reader);
    if (!res.ok()) return nullptr;
    std::shared_ptr<arrow::Table> table;
    auto res_t = file_reader->ReadTable(&table);
    if (!res_t.ok()) return nullptr;
    return table;
}

std::vector<std::shared_ptr<arrow::Table>> Loader::load_tables(
    const std::vector<const FileInfo *> &files) {
    // TODO: implement cache replacement algorithm
    //   for now we hold all of them in memory
    std::vector<std::shared_ptr<arrow::Table>> result;
    result.reserve(files.size());
    for (auto const *file : files) {
        auto entry = load_table(file->filename);
        result.emplace_back(entry);
    }

    return result;
}

}  // namespace hermes