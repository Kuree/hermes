#include "loader.hh"

#include <filesystem>
#include <fstream>
#include <iostream>

#include "arrow/api.h"
#include "arrow/filesystem/localfs.h"
#include "fmt/format.h"
#include "parquet/arrow/reader.h"
#include "parquet/statistics.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace fs = std::filesystem;

namespace hermes {

TransactionData TransactionDataIter::operator*() const {
    TransactionData data;
    data.transaction = *it_;
    data.events = std::make_shared<EventBatch>();
    auto events = loader_->get_events(*data.transaction);
    data.events->reserve(events.size());
    for (auto const &e : events) {
        data.events->emplace_back(e);
    }
    return data;
}

Loader::Loader(std::string dir) : dir_(fs::absolute(std::move(dir))) {
    fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
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
}

bool contains_value(const std::shared_ptr<parquet::Statistics> &stats, uint64_t value) {
    auto typed = std::reinterpret_pointer_cast<parquet::TypedStatistics<arrow::UInt64Type>>(stats);
    auto min = typed->min();
    auto max = typed->max();
    return max >= value && min <= value;
}

bool contains_value(const std::shared_ptr<parquet::Statistics> &stats, uint64_t min_value,
                    uint64_t max_value) {
    auto typed = std::reinterpret_pointer_cast<parquet::TypedStatistics<arrow::UInt64Type>>(stats);
    auto min = typed->min();
    auto max = typed->max();
    return min <= max_value && min_value <= max;
}

bool contains_value(const std::shared_ptr<parquet::Statistics> &min_stats,
                    const std::shared_ptr<parquet::Statistics> &max_stats, uint64_t min_value,
                    uint64_t max_value) {
    auto typed_min =
        std::reinterpret_pointer_cast<parquet::TypedStatistics<arrow::UInt64Type>>(min_stats);
    auto typed_max =
        std::reinterpret_pointer_cast<parquet::TypedStatistics<arrow::UInt64Type>>(max_stats);
    auto min = typed_min->min();
    auto max = typed_max->max();

    return min <= max_value && min_value <= max;
}

std::vector<std::unique_ptr<TransactionBatch>> Loader::get_transactions(uint64_t min_time,
                                                                        uint64_t max_time) {
    std::vector<std::pair<const FileInfo *, std::vector<uint64_t>>> files;
    for (auto const &file : transactions_) {
        auto stats = file_metadata_.at(file);
        // we use time column
        auto max_time_stats = stats.at("max_time");
        auto min_time_stats = stats.at("min_time");
        auto pair = std::make_pair(file, std::vector<uint64_t>{});
        for (uint64_t idx = 0; idx < max_time_stats.size(); idx++) {
            auto const &min_s = min_time_stats[idx];
            auto const &max_s = max_time_stats[idx];
            if (contains_value(min_s, max_s, min_time, max_time)) {
                pair.second.emplace_back(idx);
            }
        }
        files.emplace_back(pair);
    }
    // load the tables
    auto tables = load_tables(files);
    std::vector<std::unique_ptr<TransactionBatch>> result;
    result.reserve(files.size());
    for (uint64_t i = 0; i < files.size(); i++) {
        auto const &load_result = tables[i];
        auto rows = files[i].second;
        auto batch = TransactionBatch::deserialize(load_result.table, rows);
        batch->set_transaction_name(load_result.name);
        result.emplace_back(std::move(batch));
    }
    return result;
}

std::vector<std::unique_ptr<EventBatch>> Loader::get_events(uint64_t min_time, uint64_t max_time) {
    std::vector<std::pair<const FileInfo *, std::vector<uint64_t>>> files;
    for (auto const &file : events_) {
        auto stats = file_metadata_.at(file);
        auto time_stats = stats.at("time");
        auto pair = std::make_pair(file, std::vector<uint64_t>{});
        for (uint64_t idx = 0; idx < time_stats.size(); idx++) {
            auto const &s = time_stats[idx];
            if (contains_value(s, min_time, max_time)) {
                pair.second.emplace_back(idx);
            }
        }
        files.emplace_back(pair);
    }
    // load the tables
    auto tables = load_tables(files);
    std::vector<std::unique_ptr<EventBatch>> result;
    result.reserve(files.size());
    for (uint64_t i = 0; i < files.size(); i++) {
        auto const &load_result = tables[i];
        auto rows = files[i].second;
        auto batch = EventBatch::deserialize(load_result.table, rows);
        batch->set_event_name(load_result.name);
        result.emplace_back(std::move(batch));
    }
    return result;
}

EventBatch Loader::get_events(const Transaction &transaction) {
    std::unordered_map<const FileInfo *, std::vector<uint64_t>> files;
    auto const &ids = transaction.events();
    for (auto const &file : events_) {
        auto stats = file_metadata_.at(file);
        auto id_stats = stats.at("id");
        for (uint64_t chunk_idx = 0; chunk_idx < id_stats.size(); chunk_idx++) {
            auto const &id_stat = id_stats[chunk_idx];
            for (auto id : ids) {
                if (contains_value(id_stat, id)) {
                    files[file].emplace_back(chunk_idx);
                }
            }
        }
    }

    std::unordered_map<uint64_t, Event *> id_mapping;
    for (auto const &[file, chunk_ids] : files) {
        auto table = load_table(file);
        for (auto const &chunk_id : chunk_ids) {
            auto *events = load_events(table, chunk_id);
            for (auto const id : ids) {
                if (id_mapping.find(id) != id_mapping.end()) {
                    // we already found it
                    continue;
                }
                auto *e = events->get_event(id);
                if (e) {
                    // we found it
                    id_mapping.emplace(id, e);
                }
            }
        }
    }
    EventBatch result;
    result.resize(transaction.events().size(), nullptr);
    for (auto i = 0; i < result.size(); i++) {
        auto const id = transaction.events()[i];
        if (id_mapping.find(id) != id_mapping.end()) {
            result[i] = id_mapping.at(id)->shared_from_this();
        }
    }

    return result;
}

std::shared_ptr<TransactionStream> Loader::get_transaction_stream(const std::string &name) {
    // need to find files
    const FileInfo *info = nullptr;
    for (auto const &file : files_) {
        if (file->name == name && file->type == FileInfo::FileType::transaction) {
            info = file.get();
            break;
        }
    }
    if (!info) return nullptr;
    auto table = load_table(info);
    auto transactions = TransactionBatch::deserialize(table);
    if (transactions) {
        std::shared_ptr<TransactionBatch> ptr = std::move(transactions);
        return std::make_shared<TransactionStream>(ptr, this);
    } else {
        return nullptr;
    }
}

[[maybe_unused]] void Loader::print_files() const {
    for (auto const &file : files_) {
        std::cout << "File: " << std::filesystem::absolute(file->filename) << std::endl
                  << '\t'
                  << "Type: " << (file->type == FileInfo::FileType::event ? "event" : "transaction")
                  << std::endl;
    }
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

    parquet_file = fs::path(path).parent_path() / parquet_file;
    auto info = std::make_unique<FileInfo>(
        type == "event" ? FileInfo::FileType::event : FileInfo::FileType::transaction,
        parquet_file);
    if (type == "event") {
        events_.emplace_back(info.get());
    } else if (type == "transaction") {
        transactions_.emplace_back(info.get());
    } else {
        return;
    }
    // get name
    auto name_opt = get_member<std::string>(document, "name");
    if (name_opt) {
        info->name = *name_opt;
    }

    // load table as well
    // notice that we don't actually load the entire table into memory, just
    // indices and references
    load_table(info.get());

    files_.emplace_back(std::move(info));
}

std::shared_ptr<arrow::Table> Loader::load_table(const FileInfo *file) {
    if (tables_.find(file) != tables_.end()) {
        return tables_.at(file);
    }
    auto res_f = fs_->OpenInputFile(file->filename);
    if (!res_f.ok()) return nullptr;
    auto f = *res_f;
    auto *pool = arrow::default_memory_pool();
    std::unique_ptr<parquet::arrow::FileReader> file_reader;
    auto res = parquet::arrow::OpenFile(f, pool, &file_reader);
    if (!res.ok()) return nullptr;
    std::shared_ptr<arrow::Table> table;
    auto res_t = file_reader->ReadTable(&table);
    if (!res_t.ok()) return nullptr;
    tables_.emplace(file, table);

    // also load statistic
    auto metadata = file_reader->parquet_reader()->metadata();
    auto num_row_groups = metadata->num_row_groups();
    // use metadata statistics
    for (auto row_group = 0; row_group < num_row_groups; row_group++) {
        auto group_metadata = metadata->RowGroup(row_group);
        auto num_column = group_metadata->num_columns();
        auto const *schema = group_metadata->schema();

        for (auto column_idx = 0; column_idx < num_column; column_idx++) {
            auto column_name = schema->Column(column_idx)->name();
            auto column_meta = group_metadata->ColumnChunk(column_idx);
            auto stats = column_meta->statistics();
            printf("is stats set: %d\n", column_meta->is_stats_set());
            file_metadata_[file][column_name].emplace_back(stats);
        }
    }

    return table;
}

std::vector<LoaderResult> Loader::load_tables(
    const std::vector<std::pair<const FileInfo *, std::vector<uint64_t>>> &files) {
    std::vector<LoaderResult> result;
    result.reserve(files.size());
    for (auto const &[file, groups] : files) {
        auto entry = load_table(file);
        result.emplace_back(LoaderResult{entry, file->name});
    }

    return result;
}

EventBatch *Loader::load_events(const std::shared_ptr<arrow::Table> &table, uint64_t idx) {
    // TODO: implement cache replacement algorithm
    //   for now we hold all of them in memory
    auto entry = std::make_pair(table.get(), idx);
    if (event_cache_.find(entry) == event_cache_.end()) {
        auto events = EventBatch::deserialize(table, idx);
        // put it into cache
        event_cache_.emplace(entry, std::move(events));
    }
    return event_cache_.at(entry).get();
}

}  // namespace hermes