#include "loader.hh"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>

#include "arrow/api.h"
#include "arrow/filesystem/localfs.h"
#include "fmt/format.h"
#include "json.hh"
#include "parquet/arrow/reader.h"
#include "parquet/statistics.h"
#include "process.hh"
#include "pubsub.hh"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "serializer.hh"
#include "util.hh"

namespace fs = std::filesystem;

namespace hermes {

std::string FileInfo::type_str(FileType type) {
    switch (type) {
        case FileType::event:
            return "event";
        case FileType::transaction:
            return "transaction";
        case FileType::transaction_group:
            return "transaction-group";
    }
    return "";
}

void load_transaction(TransactionData &data, Loader *loader) {
    data.events = std::make_shared<EventBatch>();
    auto events = loader->get_events(*data.transaction);
    data.events->reserve(events->size());
    for (auto const &e : *events) {
        data.events->emplace_back(e);
    }
}

void load_transaction_group(TransactionData::TransactionGroupData &data, Loader *loader) {
    // assume the group is already set
    auto const &ts = data.group->transactions();
    auto const &masks = data.group->transaction_masks();
    data.values.reserve(ts.size());
    for (auto i = 0u; i < data.group->size(); i++) {
        auto tid = ts[i];
        auto is_group = masks[i];
        TransactionData d;
        if (is_group) {
            d.group = TransactionData::TransactionGroupData();
            (*d.group).group = loader->get_transaction_group(tid);
            load_transaction_group(*d.group, loader);
        } else {
            d.transaction = loader->get_transaction(tid);
            // load the event and get the events
            load_transaction(d, loader);
        }
        data.values.emplace_back(d);
    }
}

TransactionDataIter::TransactionDataIter(const TransactionStream *stream, uint64_t current_row)
    : stream_(stream), current_row_(current_row) {
    // compute the index
    compute_index();
}

void TransactionDataIter::compute_index() {
    if (current_row_ >= stream_->size()) {
        return;
    }
    // need to load transactions
    // figure out which table to load
    // depends this is a index-mapped tables or not, we need to compute the table
    // differently
    if (stream_->row_mapping_) {
        // compute the table and index
        // maybe a slightly more optimized way to compute this
        auto const &mapping = *stream_->row_mapping_;
        uint64_t table_index = 0;
        table_index_ = current_row_;
        for (table_index = 0; table_index < mapping.size(); table_index++) {
            if (table_index_ >= mapping[table_index].size()) {
                table_index_ -= mapping[table_index].size();
            } else {
                break;
            }
        }
        auto it = stream_->tables_.begin();
        std::advance(it, table_index);
        table_entry_ = it->second;
        // use the mapping
        table_index_ = mapping[table_index][table_index_];
    } else {
        auto table_iter = stream_->tables_.upper_bound(current_row_);
        table_entry_ = table_iter->second;
        // compute the index
        auto offset = table_iter->first - current_row_;
        table_index_ = table_entry_.second->num_rows() - offset;
    }
}

TransactionData TransactionDataIter::operator*() const {
    TransactionData data;
    if (current_row_ >= stream_->size()) {
        return data;
    }

    auto const &[is_group, table] = table_entry_;

    if (is_group) {
        auto groups = stream_->loader_->load_transaction_groups(table);
        data.group = TransactionData::TransactionGroupData();
        data.group->group = (*groups)[table_index_];

        // recursively construct the values
        load_transaction_group(*data.group, stream_->loader_);
    } else {
        auto transactions = stream_->loader_->load_transactions(table);
        data.transaction = (*transactions)[table_index_];
        load_transaction(data, stream_->loader_);
    }

    return data;
}

TransactionStream::TransactionStream(
    const std::vector<std::pair<bool, const arrow::Table *>> &tables, Loader *loader)
    : loader_(loader) {
    num_entries_ = 0;
    for (auto const &[group, table] : tables) {
        num_entries_ += table->num_rows();
        tables_.emplace(num_entries_, std::make_pair(group, table));
    }
}

TransactionStream TransactionStream::where(
    const std::function<bool(const TransactionData &)> &filter) const {
    // we split jobs on the tables.
    std::vector<std::vector<uint64_t>> row_mapping;
    // get original tables
    std::vector<std::pair<bool, const arrow::Table *>> tables;
    tables.reserve(tables_.size());
    for (auto const &iter : tables_) {
        tables.emplace_back(iter.second);
    }
    row_mapping.resize(tables_.size());

    std::vector<std::thread> threads;
    threads.reserve(tables.size());
    for (uint64_t idx = 0; idx < tables.size(); idx++) {  // NOLINT
        auto const &table_entry = tables[idx];
        // assume the result will be 1/3 or the original size for each
        // table
        row_mapping.reserve(table_entry.second->num_rows() / 3);
        threads.emplace_back(std::thread([idx, table_entry, filter, &row_mapping, this]() {
            uint64_t i = 0;
            TransactionStream stream;
            if (row_mapping_) {
                stream = TransactionStream({table_entry}, loader_, {(*row_mapping_)[idx]});
            } else {
                stream = TransactionStream({table_entry}, loader_);
            }
            for (auto const &data : stream) {
                if (filter(data)) {
                    if (row_mapping_) {
                        row_mapping[idx].emplace_back((*row_mapping_)[idx][i]);
                    } else {
                        row_mapping[idx].emplace_back(i);
                    }
                }
                i++;
            }
        }));
    }

    for (auto &thread : threads) thread.join();

    return TransactionStream(tables, loader_, row_mapping);
}

rapidjson::Value get_json_value(const TransactionData &data,
                                rapidjson::MemoryPoolAllocator<> &allocator) {
    rapidjson::Value value(rapidjson::kObjectType);
    if (data.is_group()) {
        json::set_member(value, allocator, "name", data.group->group->name());
        json::set_member(value, allocator, "start", data.group->group->start_time());
        json::set_member(value, allocator, "end", data.group->group->end_time());
        json::set_member(value, allocator, "finished", data.group->group->finished());
        json::set_member(value, allocator, "type", "transaction-group");
        json::set_member(value, allocator, "id", data.group->group->id());

        rapidjson::Value members(rapidjson::kArrayType);
        for (auto const &d : data.group->values) {
            auto v = get_json_value(d, allocator);
            members.PushBack(std::move(v), allocator);
        }
        json::set_member(value, allocator, "group", members);
    } else {
        json::set_member(value, allocator, "name", data.transaction->name());
        json::set_member(value, allocator, "start", data.transaction->start_time());
        json::set_member(value, allocator, "end", data.transaction->end_time());
        json::set_member(value, allocator, "finished", data.transaction->finished());
        json::set_member(value, allocator, "type", "transaction");
        json::set_member(value, allocator, "id", data.transaction->id());
        // serialize events
        rapidjson::Value member = json::serialize(allocator, data.events);
        json::set_member(value, allocator, "events", member);
    }
    return value;
}

std::string TransactionStream::json() const {
    rapidjson::Value value(rapidjson::kArrayType);
    auto allocator = rapidjson::MemoryPoolAllocator<>();
    for (auto &&data : *this) {
        auto v = get_json_value(data, allocator);
        value.PushBack(v, allocator);
    }

    return json::serialize(value, false);
}

TransactionStream::TransactionStream(
    const std::vector<std::pair<bool, const arrow::Table *>> &tables, Loader *loader,
    std::vector<std::vector<uint64_t>> row_mapping)
    : loader_(loader), row_mapping_(std::move(row_mapping)) {
    num_entries_ = 0;
    for (uint64_t i = 0; i < tables.size(); i++) {
        auto const &maps = (*row_mapping_)[i];
        num_entries_ += maps.size();
        tables_.emplace(num_entries_, tables[i]);
    }
}

Loader::Loader(const std::string &dir) : Loader(std::vector<std::string>{dir}) {}

Loader::Loader(const std::vector<std::string> &dirs) {
    for (auto const &dir : dirs) {
        open_dir(FileSystemInfo(dir));
    }

    // compute stats. this should be very fast
    compute_stats();
    init_cache();
    compute_event_id_index();
}

Loader::Loader(const std::vector<FileSystemInfo> &infos) {
    for (auto const &info : infos) {
        open_dir(info);
    }

    // compute stats. this should be very fast
    compute_stats();
    init_cache();
    compute_event_id_index();
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

std::shared_ptr<Transaction> Loader::get_transaction(uint64_t id) {
    // find out ids
    std::vector<std::pair<const FileInfo *, std::vector<uint64_t>>> files;
    for (auto const *file : transactions_) {
        auto stats = file_metadata_.at(file);
        auto id_stats = stats.at("id");
        std::vector<uint64_t> chunks;
        for (uint64_t i = 0; i < id_stats.size(); i++) {
            if (contains_value(id_stats[i], id)) {
                // we assume that most of the ids are stored together, so this method
                // should be very fast actually
                chunks.emplace_back(i);
            }
        }
        files.emplace_back(std::make_pair(file, chunks));
    }
    if (files.empty()) return nullptr;
    auto tables = load_tables(files);
    // we expect there is only one entry
    for (auto const &table : tables) {
        auto t = load_transactions(table.table);
        if (t->contains(id)) {
            return t->at(id);
        }
    }
    return nullptr;
}

std::shared_ptr<TransactionGroup> Loader::get_transaction_group(uint64_t id) {
    // TODO: refactor this
    // find out ids
    std::vector<std::pair<const FileInfo *, std::vector<uint64_t>>> files;
    for (auto const *file : transaction_groups_) {
        auto stats = file_metadata_.at(file);
        auto id_stats = stats.at("id");
        std::vector<uint64_t> chunks;
        for (uint64_t i = 0; i < id_stats.size(); i++) {
            if (contains_value(id_stats[i], id)) {
                // we assume that most of the ids are stored together, so this method
                // should be very fast actually
                chunks.emplace_back(i);
            }
        }
        files.emplace_back(std::make_pair(file, chunks));
    }
    if (files.empty()) return nullptr;
    auto tables = load_tables(files);
    // we expect there is only one entry
    for (auto const &table : tables) {
        auto t = load_transaction_groups(table.table);
        if (t->contains(id)) {
            return t->at(id);
        }
    }
    return nullptr;
}

std::vector<std::shared_ptr<TransactionBatch>> Loader::get_transactions(uint64_t min_time,
                                                                        uint64_t max_time) {
    // load the tables
    auto tables = load_transaction_table(std::nullopt, min_time, max_time);
    std::vector<std::shared_ptr<TransactionBatch>> result;
    result.reserve(tables.size());
    for (auto const &load_result : tables) {
        auto batch = load_transactions(load_result.table);
        batch->set_name(load_result.name);
        result.emplace_back(std::move(batch));
    }
    return result;
}

template <typename T>
std::shared_ptr<T> merge_batch(const std::shared_ptr<T> &left, const std::shared_ptr<T> right,
                               const std::string &name) {
    auto result = std::make_shared<T>();
    result->reserve(left->size() + right->size());
    result->insert(result->end(), left->begin(), left->end());
    result->insert(result->end(), right->begin(), right->end());
    result->set_name(name);
    return result;
}

std::shared_ptr<TransactionBatch> Loader::get_transactions(const std::string &name,
                                                           uint64_t min_time, uint64_t max_time) {
    auto tables = load_transaction_table(name, min_time, max_time);
    std::shared_ptr<TransactionBatch> result;
    for (auto const &load_result : tables) {
        auto batch = load_transactions(load_result.table);
        if (!result) {
            result = batch;
        } else {
            result = merge_batch(result, batch, load_result.name);
        }
    }
    return result;
}

std::shared_ptr<TransactionGroupBatch> Loader::get_transaction_groups(const std::string &name,
                                                                      uint64_t min_time,
                                                                      uint64_t max_time) {
    auto tables = load_transaction_group_table(name, min_time, max_time);
    std::shared_ptr<TransactionGroupBatch> result;
    for (auto const &load_result : tables) {
        auto batch = load_transaction_groups(load_result.table);
        if (!result) {
            result = batch;
        } else {
            result = merge_batch(result, batch, load_result.name);
        }
    }
    return result;
}

std::shared_ptr<TransactionBatch> Loader::get_transactions(
    const std::shared_ptr<Transaction> &transaction) {
    auto const id = transaction->id();
    std::vector<std::pair<const FileInfo *, std::vector<uint64_t>>> files;
    for (auto const *file : transactions_) {
        if (file->type == FileInfo::FileType::transaction) {
            auto stats = file_metadata_.at(file);
            auto id_stats = stats.at("id");
            std::vector<uint64_t> chunks;
            for (uint64_t i = 0; i < id_stats.size(); i++) {
                if (contains_value(id_stats[i], id)) {
                    // we assume that most of the ids are stored together, so this method
                    // should be very fast actually
                    chunks.emplace_back(i);
                }
            }
            files.emplace_back(std::make_pair(file, chunks));
        }
    }
    if (files.empty()) return nullptr;
    auto tables = load_tables(files);
    // we expect there is only one entry
    for (auto const &table : tables) {
        auto t = load_transactions(table.table);
        if (t->contains(id)) {
            return t;
        }
    }
    return nullptr;
}

std::vector<std::shared_ptr<EventBatch>> Loader::get_events(uint64_t min_time, uint64_t max_time) {
    auto tables = load_events_table(min_time, max_time);
    std::vector<std::shared_ptr<EventBatch>> result;
    result.reserve(tables.size());
    for (auto const &load_result : tables) {
        auto batch = load_events(load_result.table);
        batch->set_name(load_result.name);
        result.emplace_back(std::move(batch));
    }
    return result;
}

std::shared_ptr<EventBatch> Loader::get_events(const std::string &name, uint64_t min_time,
                                               uint64_t max_time) {
    std::vector<std::pair<const FileInfo *, std::vector<uint64_t>>> files;
    for (auto const &file : events_) {
        if (file->name != name) {
            continue;
        }
        auto stats = file_metadata_.at(file);
        auto pair = std::make_pair(file, std::vector<uint64_t>{});
        for (uint64_t idx = 0; idx < file_metadata_.size(file); idx++) {
            if (contains_time(file, idx, min_time, max_time)) {
                pair.second.emplace_back(idx);
            }
        }
        files.emplace_back(pair);
    }
    auto tables = load_tables(files);
    std::shared_ptr<EventBatch> result;
    for (auto const &load_result : tables) {
        auto batch = load_events(load_result.table);
        if (!result) {
            result = batch;
            result->set_name(load_result.name);
        } else {
            // since the old one is used for caching
            // we have to create new result instead of reusing the old one
            auto temp = result;
            result = std::make_shared<EventBatch>();
            result->reserve(temp->size() + batch->size());
            result->insert(result->end(), temp->begin(), temp->end());
            result->insert(result->end(), batch->begin(), batch->end());
            result->set_name(load_result.name);
        }
    }

    return result;
}

std::shared_ptr<EventBatch> Loader::get_events(const Transaction &transaction) {
    auto result = std::make_shared<EventBatch>();
    result->resize(transaction.events().size(), nullptr);
    if (event_id_index_.empty()) return result;

    for (auto i = 0u; i < result->size(); i++) {
        auto const id = transaction.events()[i];
        // need to search for the correct able
        auto iter = event_id_index_.lower_bound(id);
        if (iter == event_id_index_.end()) iter--;

        while (iter != event_id_index_.end()) {
            auto temp = iter;
            auto const &target_iter = iter->first > id ? --temp : iter;
            // we expect this loop only run once for a well-formed table
            auto const *table = tables_.at(target_iter->second).get();
            auto const &events = load_events(table);
            auto *e = events->get_event(id);
            if (e) {
                (*result)[i] = e->shared_from_this();
                break;
            }
            iter++;
        }
    }

    return result;
}

std::shared_ptr<TransactionStream> Loader::get_transaction_stream(const std::string &name) {
    // all of them
    return get_transaction_stream(name, 0, std::numeric_limits<uint64_t>::max());
}

std::shared_ptr<TransactionStream> Loader::get_transaction_stream(const std::string &name,
                                                                  uint64_t start_time,
                                                                  uint64_t end_time) {
    // need to find files
    std::unordered_set<const FileInfo *> files;
    for (auto const &file : files_) {
        if (file->name == name && (file->type == FileInfo::FileType::transaction ||
                                   file->type == FileInfo::FileType::transaction_group)) {
            files.emplace(file.get());
        }
    }
    if (files.empty()) return nullptr;
    // need to gather all the tables given the info
    std::vector<std::pair<bool, const arrow::Table *>> tables;
    for (auto const &[entry, table] : tables_) {
        auto const &[f, c_id] = entry;
        // need to make sure we have that range
        if (!contains_time(f, c_id, start_time, end_time)) continue;
        if (files.find(f) != files.end()) {
            tables.emplace_back(
                std::make_pair(f->type == FileInfo::FileType::transaction_group, table.get()));
        }
    }

    if (!tables.empty()) {
        return std::make_shared<TransactionStream>(tables, this);
    } else {
        return nullptr;
    }
}

[[maybe_unused]] void Loader::print_files() const {
    for (auto const &file : files_) {
        // get file size
        std::cout << "File: " << file->filename << std::endl
                  << '\t' << "Type: " << FileInfo::type_str(file->type) << std::endl
                  << '\t' << "Size: " << file->size << std::endl;
        // compute the in memory size
        // we don't store it since it's unnecessary for normal use
        // we don't expect print_files will be called often
        uint64_t total_size = 0;
        uint64_t num_chunks = 0;
        for (auto const &[info, table] : tables_) {
            if (info.first == file.get()) {
                total_size += compute_table_size_in_memory(table);
                num_chunks++;
            }
        }
        std::cout << '\t' << "Num. of Chunks: " << num_chunks << std::endl;
        std::cout << '\t' << "Estimated size in memory: " << total_size << std::endl;
    }
}

void Loader::preload() {
    // preload until the cache is full
    // use thread pool implementation
    // maximize the load speed
    auto pool = ThreadPool(std::thread::hardware_concurrency());
    std::vector<std::future<void>> results;
    for (auto const &iter : tables_) {
        auto func = [iter, this]() {
            auto const &[file_info, table] = iter;
            auto const &[file, blk] = file_info;
            switch (file->type) {
                case FileInfo::FileType::event: {
                    load_events(table.get());
                    break;
                }
                case FileInfo::FileType::transaction: {
                    load_transactions(table.get());
                    break;
                }
                case FileInfo::FileType::transaction_group: {
                    load_transaction_groups(table.get());
                    break;
                }
            }
        };
        auto entry = pool.enqueue(func);
        results.emplace_back(std::move(entry));
    }
    for (auto &&r : results) {
        r.get();
    }
    // only if all the tables are in memory
    uint64_t num_cache_entry =
        event_cache_->size() + transaction_cache_->size() + transaction_group_cache_->size();
    preloaded_ = num_cache_entry == tables_.size();
}

void Loader::open_dir(const FileSystemInfo &info) {
    // need to detect if it's a local file system or not
    // if it doesn't contain the uri ://, then it's local filesystem
    auto fs = load_fs(info);
    if (!fs) return;

    // load the checkpoint files
    auto checkpoint_filename = Serializer::get_checkpoint_filename(info.path);
    auto fs_res = fs->OpenInputFile(checkpoint_filename);
    if (!fs_res.ok()) return;

    // load into string
    auto files = load_checkpoint_info(*fs_res);
    // multi-thread loading
    std::vector<std::thread> threads;
    threads.reserve(files.size());
    for (auto const &filename : files) {
        auto json_filename = fmt::format("{0}/{1}", info.path, filename);
        threads.emplace_back(
            std::thread([json_filename, fs, this]() { load_json(json_filename, fs); }));
    }

    // join
    for (auto &t : threads) {
        t.join();
    }
}

void Loader::load_json(const std::string &json_info,
                       const std::shared_ptr<arrow::fs::FileSystem> &fs) {
    auto file_res = fs->OpenInputFile(json_info);
    if (!file_res.ok()) return;
    auto file = *file_res;
    // read out the entire content
    auto size_res = file->GetSize();
    if (!size_res.ok()) return;
    auto file_size = *size_res;
    auto buffer_res = file->Read(file_size);
    if (!buffer_res.ok()) return;
    auto buffer = *buffer_res;
    auto content = buffer->ToString();

    rapidjson::Document document;
    document.Parse(content.c_str());
    if (document.HasParseError()) return;
    // get indexed value
    auto opt_parquet_file = json::get_member<std::string>(document, "parquet");
    if (!opt_parquet_file) return;
    auto parquet_file = *opt_parquet_file;

    auto opt_type = json::get_member<std::string>(document, "type");
    if (!opt_type) return;
    auto const &type = *opt_type;

    parquet_file = fs::path(json_info).parent_path() / parquet_file;
    // make sure we can actually open this file
    auto table_file_res = fs->OpenInputFile(parquet_file);
    if (!table_file_res.ok()) return;
    auto table_file = *table_file_res;
    size_res = table_file->GetSize();
    if (!size_res.ok()) return;
    file_size = *size_res;

    FileInfo::FileType file_type;
    if (type == "event")
        file_type = FileInfo::FileType::event;
    else if (type == "transaction")
        file_type = FileInfo::FileType::transaction;
    else if (type == "transaction-group")
        file_type = FileInfo::FileType::transaction_group;
    else
        return;

    auto info = std::make_unique<FileInfo>(file_type, parquet_file);
    {
        std::lock_guard guard(files_mutex_);

        switch (file_type) {
            case FileInfo::FileType::event: {
                events_.emplace_back(info.get());
                break;
            }
            case FileInfo::FileType::transaction: {
                transactions_.emplace_back(info.get());
                break;
            }
            case FileInfo::FileType::transaction_group: {
                transaction_groups_.emplace_back(info.get());
                break;
            }
        }
    }

    // get name
    auto name_opt = json::get_member<std::string>(document, "name");
    if (name_opt) {
        info->name = *name_opt;
    }
    // size
    info->size = file_size;
    // load table as well
    // notice that we don't actually load the entire table into memory, just
    // indices and references
    preload_table(info.get(), table_file);

    {
        std::lock_guard guard(files_mutex_);
        files_.emplace_back(std::move(info));
    }
}

bool Loader::preload_table(const FileInfo *file,
                           const std::shared_ptr<arrow::io::RandomAccessFile> &f) {
    auto *pool = arrow::default_memory_pool();
    std::unique_ptr<parquet::arrow::FileReader> file_reader;
    auto res = parquet::arrow::OpenFile(f, pool, &file_reader);
    if (!res.ok()) return false;
    // load each chunks
    auto num_row_groups = file_reader->num_row_groups();
    for (int row_group_id = 0; row_group_id < num_row_groups; row_group_id++) {
        std::shared_ptr<arrow::Table> table;
        auto res_t = file_reader->ReadRowGroup(row_group_id, &table);
        if (!res_t.ok()) {
            std::cerr << "[ERROR]: " << res_t.ToString() << std::endl;
            return false;
        }
        {
            std::lock_guard guard(files_mutex_);
            tables_.emplace(std::make_pair(file, row_group_id), table);
        }
    }

    // also load statistic
    auto metadata = file_reader->parquet_reader()->metadata();
    // use metadata statistics
    for (auto row_group = 0; row_group < num_row_groups; row_group++) {
        auto group_metadata = metadata->RowGroup(row_group);
        auto num_column = group_metadata->num_columns();
        auto const *schema = group_metadata->schema();

        for (auto column_idx = 0; column_idx < num_column; column_idx++) {
            auto column_name = schema->Column(column_idx)->name();
            auto column_meta = group_metadata->ColumnChunk(column_idx);
            auto stats = column_meta->statistics();
            {
                std::lock_guard guard(files_mutex_);
                file_metadata_[file][column_name].emplace_back(stats);
            }
        }
    }

    return true;
}

std::vector<LoaderResult> Loader::load_tables(
    const std::vector<std::pair<const FileInfo *, std::vector<uint64_t>>> &files) {
    std::vector<LoaderResult> result;
    result.reserve(files.size());
    for (auto const &[file, groups] : files) {
        for (auto const &group_id : groups) {
            auto const *entry = tables_.at(std::make_pair(file, group_id)).get();
            result.emplace_back(LoaderResult{entry, file->name});
        }
    }

    return result;
}

std::shared_ptr<EventBatch> Loader::load_events(const arrow::Table *table) {
    // if everything is preloaded, go ahead and directly return the values
    if (preloaded_) {
        return event_cache_->get(table);
    }
    // we need to be very careful about locking and unlocking and also achieve high-performance
    event_cache_mutex_.lock();
    if (!event_cache_->exists(table)) {
        event_cache_mutex_.unlock();

        // time consuming section
        std::shared_ptr<EventBatch> events = EventBatch::deserialize(table);
        events->build_id_index();

        // put it into cache
        event_cache_mutex_.lock();
        event_cache_->put(table, events);
        event_cache_mutex_.unlock();
        return events;
    } else {
        auto r = event_cache_->get(table);
        event_cache_mutex_.unlock();
        return r;
    }
}

std::shared_ptr<TransactionBatch> Loader::load_transactions(const arrow::Table *table) {
    // similar logic to transaction cache as the load events
    transaction_cache_mutex_.lock();
    if (!transaction_cache_->exists(table)) {
        transaction_cache_mutex_.unlock();

        // time consuming section
        std::shared_ptr<TransactionBatch> transactions = TransactionBatch::deserialize(table);
        transactions->build_id_index();

        // put it into the cache
        transaction_cache_mutex_.lock();
        transaction_cache_->put(table, transactions);
        transaction_cache_mutex_.unlock();

        return transactions;
    } else {
        auto r = transaction_cache_->get(table);
        transaction_cache_mutex_.unlock();

        return r;
    }
}

std::shared_ptr<TransactionGroupBatch> Loader::load_transaction_groups(const arrow::Table *table) {
    // same logic
    transaction_group_cache_mutex_.lock();
    if (!transaction_group_cache_->exists(table)) {
        transaction_group_cache_mutex_.unlock();

        // time consuming section
        std::shared_ptr<TransactionGroupBatch> group = TransactionGroupBatch::deserialize(table);
        group->build_index();

        transaction_group_cache_mutex_.lock();
        transaction_group_cache_->put(table, group);
        transaction_group_cache_mutex_.unlock();

        return group;
    } else {
        auto r = transaction_group_cache_->get(table);
        transaction_group_cache_mutex_.unlock();

        return r;
    }
}

void Loader::compute_stats() {
    std::unordered_set<const FileInfo *> seen_files;
    for (auto const &[info, table] : tables_) {
        auto const &[file, blk_id] = info;
        switch (file->type) {
            case FileInfo::FileType::event: {
                stats_.num_event_files++;
                stats_.num_events += table->num_rows();
                // need to load the column stats

                auto const &stats = file_metadata_.at(file).at("time")[blk_id];
                auto typed =
                    std::reinterpret_pointer_cast<parquet::TypedStatistics<arrow::UInt64Type>>(
                        stats);
                auto min = typed->min();
                auto max = typed->max();

                if (min < stats_.min_event_time) {
                    stats_.min_event_time = min;
                }

                if (max > stats_.max_event_time) {
                    stats_.max_event_time = max;
                }
                stats_.average_event_chunk_size += compute_table_size_in_memory(table);
                break;
            }
            case FileInfo::FileType::transaction: {
                stats_.num_transaction_files++;
                stats_.num_transactions += table->num_rows();
                stats_.average_transaction_chunk_size += compute_table_size_in_memory(table);
                break;
            }
            case FileInfo::FileType::transaction_group: {
                stats_.num_transaction_group_files++;
                stats_.num_transaction_groups += table->num_rows();
                stats_.average_transaction_group_chunk_size += compute_table_size_in_memory(table);
                break;
            }
        }
    }

    // actually compute the average
    if (stats_.num_event_files > 0) {
        stats_.average_event_chunk_size = stats_.average_event_chunk_size / stats_.num_event_files;
    }
    if (stats_.num_transaction_files > 0) {
        stats_.average_transaction_chunk_size =
            stats_.average_transaction_chunk_size / stats_.num_transaction_files;
    }
    if (stats_.num_transaction_group_files > 0) {
        stats_.average_transaction_group_chunk_size =
            stats_.average_transaction_group_chunk_size / stats_.num_transaction_group_files;
    }
}

void Loader::stream(bool stream_transactions) {
    stream(MessageBus::default_bus(), stream_transactions);
}

BatchSchema Loader::get_event_schema(const std::string &name) {
    // we put some good faith that the files with the same name will be
    // consistent
    BatchSchema result;
    const FileInfo *file_ = nullptr;
    for (auto const &file : files_) {
        if (file->type == FileInfo::FileType::event && file->name == name) {
            file_ = file.get();
            break;
        }
    }
    if (!file_) return result;
    auto const &table = tables_.at(std::make_pair(file_, 0));
    auto const &schema = table->schema();
    auto const &names = schema->field_names();
    for (auto const &n : names) {
        auto field = schema->GetFieldByName(n);
        // based on the field type
        if (field->type()->Equals(arrow::boolean())) {
            result.emplace(n, EventDataType::bool_);
        } else if (field->type()->Equals(arrow::uint8())) {
            result.emplace(n, EventDataType::uint8_t_);
        } else if (field->type()->Equals(arrow::uint16())) {
            result.emplace(n, EventDataType::uint16_t_);
        } else if (field->type()->Equals(arrow::uint32())) {
            result.emplace(n, EventDataType::uint32_t_);
        } else if (field->type()->Equals(arrow::uint64())) {
            result.emplace(n, EventDataType::uint64_t_);
        } else if (field->type()->Equals(arrow::utf8())) {
            result.emplace(n, EventDataType::string);
        }
    }

    return result;
}

std::set<std::string> get_names(const std::vector<std::unique_ptr<FileInfo>> &files,
                                FileInfo::FileType type) {
    std::set<std::string> result;
    for (auto const &info : files) {
        if (info->type == type) {
            result.emplace(info->name);
        }
    }
    return result;
}

std::set<std::string> Loader::get_event_names() const {
    return get_names(files_, FileInfo::FileType::event);
}

std::set<std::string> Loader::get_transaction_names() const {
    return get_names(files_, FileInfo::FileType::transaction);
}

std::set<std::string> Loader::get_transaction_group_names() const {
    return get_names(files_, FileInfo::FileType::transaction_group);
}

template <typename T>
void should_stop(const std::vector<std::shared_ptr<T>> &values,
                 const std::vector<uint64_t> &indices, bool &stop) {
    if (!values.empty()) {
        for (uint64_t i = 0; i < values.size(); i++) {
            if (indices[i] < values[i]->size()) {
                stop = false;
            }
        }
    }
}

template <typename T>
void stream_values(std::vector<std::shared_ptr<T>> &values, std::vector<uint64_t> &indices,
                   MessageBus *bus) {
    if (!values.empty()) {
        uint32_t min_index = 0;
        auto value = (*values[min_index])[indices[min_index]];
        for (uint64_t i = 1; i < values.size(); i++) {
            if constexpr (std::is_same<T, EventBatch>::value) {
                if ((*values[i])[indices[i]]->time() < value->time()) {
                    min_index = i;
                    value = (*values[i])[indices[i]];
                }
            } else if constexpr (std::is_same<T, TransactionBatch>::value ||
                                 std::is_same<T, TransactionGroupBatch>::value) {
                if ((*values[i])[indices[i]]->start_time() < value->start_time()) {
                    min_index = i;
                    value = (*values[i])[indices[i]];
                }
            }
        }
        indices[min_index]++;
        bus->publish(values[min_index]->name(), value);

        // need to be careful about the boundary
        if (indices[min_index] >= values[min_index]->size()) {
            // need to pop that entry
            values.erase(values.begin() + min_index);
            indices.erase(indices.begin() + min_index);
        }
    }
}

template <typename T>
void load_values(const std::vector<LoaderResult> &load_results,
                 std::vector<std::shared_ptr<T>> &values,
                 const std::function<std::shared_ptr<T>(const arrow::Table *)> &load_func) {
    for (auto const &res : load_results) {
        auto const &table = res.table;

        // load the table
        auto event_batch = load_func(table);
        event_batch->set_name(res.name);
        values.emplace_back(std::move(event_batch));
    }
}

void Loader::stream(MessageBus *bus, bool stream_transactions) {
    // we stream out every items roughly based on the time
    // because each table is a chunk, as long as the cache doesn't
    // take much memory, we're good

    // we load them in approximation of time, it won't be perfect but should be good
    // enough

    auto load_results = load_events_table(0, std::numeric_limits<uint64_t>::max());
    std::vector<std::shared_ptr<EventBatch>> events;
    std::vector<std::shared_ptr<TransactionBatch>> transactions;
    std::vector<std::shared_ptr<TransactionGroupBatch>> transaction_groups;

    // gcc failed to induce the template type
    load_values<EventBatch>(load_results, events,
                            [this](const arrow::Table *table) { return load_events(table); });

    // decide whether to stream transactions or not
    if (stream_transactions) {
        load_results =
            load_transaction_table(std::nullopt, 0, std::numeric_limits<uint64_t>::max());

        load_values<TransactionBatch>(
            load_results, transactions,
            [this](const arrow::Table *table) { return load_transactions(table); });

        // groups as well
        load_results =
            load_transaction_group_table(std::nullopt, 0, std::numeric_limits<uint64_t>::max());
        load_values<TransactionGroupBatch>(
            load_results, transaction_groups,
            [this](const arrow::Table *table) { return load_transaction_groups(table); });
    }

    std::vector<uint64_t> event_indices;
    event_indices.resize(events.size(), 0);
    std::vector<uint64_t> transaction_indices;
    transaction_indices.resize(transactions.size(), 0);
    std::vector<uint64_t> transaction_group_indices;
    transaction_group_indices.resize(transactions.size(), 0);

    // stream out the events and transactions

    while (true) {
        bool stop = true;
        should_stop(events, event_indices, stop);
        should_stop(transactions, transaction_indices, stop);
        should_stop(transaction_groups, transaction_group_indices, stop);

        if (stop) break;

        // stream events
        stream_values(events, event_indices, bus);
        stream_values(transactions, transaction_indices, bus);
        stream_values(transaction_groups, transaction_group_indices, bus);
    }
}

std::vector<LoaderResult> Loader::load_events_table(uint64_t min_time, uint64_t max_time) {
    std::vector<std::pair<const FileInfo *, std::vector<uint64_t>>> files;
    for (auto const &file : events_) {
        auto stats = file_metadata_.at(file);
        auto pair = std::make_pair(file, std::vector<uint64_t>{});
        for (uint64_t idx = 0; idx < file_metadata_.size(file); idx++) {
            if (contains_time(file, idx, min_time, max_time)) {
                pair.second.emplace_back(idx);
            }
        }
        files.emplace_back(pair);
    }
    // load the tables
    auto tables = load_tables(files);
    return tables;
}

std::vector<LoaderResult> Loader::load_transaction_table(const std::optional<std::string> &name,
                                                         uint64_t min_time, uint64_t max_time) {
    return load_batch_table(transactions_, name, min_time, max_time);
}

std::vector<LoaderResult> Loader::load_transaction_group_table(
    const std::optional<std::string> &name, uint64_t min_time, uint64_t max_time) {
    return load_batch_table(transaction_groups_, name, min_time, max_time);
}

std::vector<LoaderResult> Loader::load_batch_table(const std::vector<const FileInfo *> &info,
                                                   const std::optional<std::string> &name,
                                                   uint64_t min_time, uint64_t max_time) {
    std::vector<std::pair<const FileInfo *, std::vector<uint64_t>>> files;
    for (auto const &file : info) {
        if (name) {
            if (file->name != name) continue;
        }
        auto stats = file_metadata_.at(file);
        auto pair = std::make_pair(file, std::vector<uint64_t>{});
        for (uint64_t idx = 0; idx < file_metadata_.size(file); idx++) {
            if (contains_time(file, idx, min_time, max_time)) {
                pair.second.emplace_back(idx);
            }
        }
        files.emplace_back(pair);
    }
    // load the tables
    auto tables = load_tables(files);
    return tables;
}

bool Loader::contains_time(const FileInfo *file, uint64_t chunk_id, uint64_t min_time,
                           uint64_t max_time) {
    auto stats = file_metadata_.at(file);
    // depends on which type of files we are dealing with
    if (file->type == FileInfo::FileType::event) {
        auto time_stats = stats.at("time");
        auto const &s = time_stats[chunk_id];
        return contains_value(s, min_time, max_time);
    } else {
        auto max_time_stats = stats.at("end_time");
        auto min_time_stats = stats.at("start_time");

        auto const &min_s = min_time_stats[chunk_id];
        auto const &max_s = max_time_stats[chunk_id];
        return contains_value(min_s, max_s, min_time, max_time);
    }
}

void Loader::init_cache() {
    // initialize the cache based on the stats
    // get total memory
    auto total_mem = os::get_total_system_memory();
    // we expect the majority of the memory will be consumed by events
    total_mem = total_mem / 6;
    auto mem_events = total_mem * 3;
    auto mem_transaction = total_mem;
    auto mem_transaction_group = total_mem;
    auto num_events =
        stats_.average_event_chunk_size > 0 ? mem_events / stats_.average_event_chunk_size : 16;
    auto num_transactions = stats_.average_transaction_chunk_size > 0
                                ? mem_transaction / stats_.average_transaction_chunk_size
                                : 16;
    auto num_transaction_groups =
        stats_.average_transaction_group_chunk_size > 0
            ? mem_transaction_group / stats_.average_transaction_group_chunk_size
            : 16;
    event_cache_ =
        std::make_unique<lru_cache<const arrow::Table *, std::shared_ptr<EventBatch>>>(num_events);
    transaction_cache_ =
        std::make_unique<lru_cache<const arrow::Table *, std::shared_ptr<TransactionBatch>>>(
            num_transactions);
    transaction_group_cache_ =
        std::make_unique<lru_cache<const arrow::Table *, std::shared_ptr<TransactionGroupBatch>>>(
            num_transaction_groups);
}

void Loader::compute_event_id_index() {
    for (auto const *file : events_) {
        auto stats = file_metadata_.at(file);
        auto id_stats = stats.at("id");
        for (uint64_t chunk_id = 0; chunk_id < id_stats.size(); chunk_id++) {
            auto typed = std::reinterpret_pointer_cast<parquet::TypedStatistics<arrow::UInt64Type>>(
                id_stats[chunk_id]);
            auto min = typed->min();
            event_id_index_.emplace(min, std::make_pair(file, chunk_id));
        }
    }
}

uint64_t Loader::compute_table_size_in_memory(const std::shared_ptr<arrow::Table> &table) {
    // this is just estimate how much memory it will occupy the memory
    auto const schema = table->schema();
    uint64_t num_row = table->num_rows();
    uint64_t row_size = 0;
    auto const &names = schema->field_names();
    for (auto const &name : names) {
        row_size += name.size();
        auto const &field = schema->GetFieldByName(name);
        if (field->type()->Equals(arrow::boolean())) {
            row_size += sizeof(bool);
        } else if (field->type()->Equals(arrow::uint8())) {
            row_size += sizeof(uint8_t);
        } else if (field->type()->Equals(arrow::uint16())) {
            row_size += sizeof(uint16_t);
        } else if (field->type()->Equals(arrow::uint32())) {
            row_size += sizeof(uint32_t);
        } else if (field->type()->Equals(arrow::uint64())) {
            row_size += sizeof(uint64_t);
        } else if (field->type()->Equals(arrow::utf8())) {
            row_size += 16;
        } else if (field->type()->Equals(arrow::list(arrow::uint64()))) {
            row_size += 4 * sizeof(uint64_t);
        }
    }
    // we also consider other data structures the batch uses
    row_size +=
        sizeof(uint64_t) + sizeof(void *) + sizeof(std::unordered_map<std::string, AttributeValue>);
    auto total = row_size * num_row;
    total += sizeof(std::unordered_map<uint64_t, void *>);
    return total;
}

std::vector<std::string> Loader::load_checkpoint_info(
    const std::shared_ptr<arrow::io::RandomAccessFile> &file) {
    auto size_res = file->GetSize();
    if (!size_res.ok()) return {};
    auto file_size = *size_res;
    auto buffer_res = file->Read(file_size);
    if (!buffer_res.ok()) return {};
    auto buffer = *buffer_res;
    auto content = buffer->ToString();

    std::vector<std::string> result;
    rapidjson::Document document;
    document.Parse(content.c_str());
    if (document.HasParseError()) return {};
    auto const &array = document["files"].GetArray();
    for (auto const &fn : array) {
        const auto *filename = fn.GetString();
        result.emplace_back(filename);
    }
    return result;
}

}  // namespace hermes