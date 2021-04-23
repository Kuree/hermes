#include "loader.hh"

#include <filesystem>
#include <fstream>
#include <iostream>

#include "arrow/api.h"
#include "arrow/filesystem/localfs.h"
#include "parquet/arrow/reader.h"
#include "parquet/statistics.h"
#include "pubsub.hh"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
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
    for (auto i = 0; i < data.group->size(); i++) {
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

TransactionData TransactionDataIter::operator*() const {
    TransactionData data;
    if (current_row_ >= stream_->size()) {
        return data;
    }
    // need to load transactions
    // figure out which table to load
    auto table_iter = stream_->tables_.upper_bound(current_row_);
    auto const &[is_group, table] = table_iter->second;
    // compute the index
    auto offset = table_iter->first - current_row_;
    auto idx = table->num_rows() - offset;

    if (is_group) {
        auto groups = stream_->loader_->load_transaction_groups(table);
        data.group = TransactionData::TransactionGroupData();
        data.group->group = (*groups)[idx];

        // recursively construct the values
        load_transaction_group(*data.group, stream_->loader_);
    } else {
        auto transactions = stream_->loader_->load_transactions(table);
        data.transaction = (*transactions)[idx];
        load_transaction(data, stream_->loader_);
    }

    return data;
}

TransactionStream::TransactionStream(
    const std::vector<std::pair<bool, std::shared_ptr<arrow::Table>>> &tables, Loader *loader)
    : loader_(loader) {
    num_entries_ = 0;
    for (auto const &[group, table] : tables) {
        num_entries_ += table->num_rows();
        tables_.emplace(num_entries_, std::make_pair(group, table));
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
    auto const &ids = transaction.events();
    auto result = std::make_shared<EventBatch>();
    result->resize(transaction.events().size(), nullptr);
    if (event_id_index_.empty()) return result;

    for (auto i = 0; i < result->size(); i++) {
        auto const id = transaction.events()[i];
        // need to search for the correct able
        auto iter = event_id_index_.lower_bound(id);
        if (iter == event_id_index_.end()) iter--;

        while (iter != event_id_index_.end()) {
            auto temp = iter;
            auto target_iter = iter->first > id ? --temp : iter;
            // we expect this loop only run once for a well-formed table
            auto table = tables_.at(target_iter->second);
            auto events = load_events(table);
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
    std::vector<std::pair<bool, std::shared_ptr<arrow::Table>>> tables;
    for (auto const &[entry, table] : tables_) {
        auto const &[f, c_id] = entry;
        // need to make sure we have that range
        if (!contains_time(f, c_id, start_time, end_time)) continue;
        if (files.find(f) != files.end()) {
            tables.emplace_back(
                std::make_pair(f->type == FileInfo::FileType::transaction_group, table));
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

void Loader::open_dir(const FileSystemInfo &info) {
    // need to detect if it's a local file system or not
    // if it doesn't contain the uri ://, then it's local filesystem
    auto fs = load_fs(info);
    if (!fs) return;

    // do a LIST option
    auto selector = arrow::fs::FileSelector();
    selector.base_dir = info.path;
    auto res = fs->GetFileInfo(selector);
    if (!res.ok()) return;
    auto files = *res;
    for (auto const &file_info : files) {
        // notice that arrow's fs doesn't include .
        if (file_info.extension() == "json") {
            load_json(file_info, fs);
        }
    }
}

void Loader::load_json(const arrow::fs::FileInfo &json_info,
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
    auto opt_parquet_file = get_member<std::string>(document, "parquet");
    if (!opt_parquet_file) return;
    auto parquet_file = *opt_parquet_file;

    auto opt_type = get_member<std::string>(document, "type");
    if (!opt_type) return;
    auto const &type = *opt_type;

    parquet_file = fs::path(json_info.path()).parent_path() / parquet_file;
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
    switch (file_type) {
        case FileInfo::FileType::event: {
            events_.emplace_back(info.get());
            // also add to the stats
            stats_.average_event_chunk_size += file_size;
            break;
        }
        case FileInfo::FileType::transaction: {
            transactions_.emplace_back(info.get());
            stats_.average_transaction_chunk_size += file_size;
        }
        case FileInfo::FileType::transaction_group: {
            transaction_groups_.emplace_back(info.get());
            stats_.average_transaction_group_chunk_size += file_size;
        }
    }
    // get name
    auto name_opt = get_member<std::string>(document, "name");
    if (name_opt) {
        info->name = *name_opt;
    }
    // size
    info->size = file_size;
    // load table as well
    // notice that we don't actually load the entire table into memory, just
    // indices and references
    preload_table(info.get(), table_file);

    files_.emplace_back(std::move(info));
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
        if (!res_t.ok()) return false;
        tables_.emplace(std::make_pair(file, row_group_id), table);
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
            file_metadata_[file][column_name].emplace_back(stats);
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
            auto entry = tables_.at(std::make_pair(file, group_id));
            result.emplace_back(LoaderResult{entry, file->name});
        }
    }

    return result;
}

std::shared_ptr<EventBatch> Loader::load_events(const std::shared_ptr<arrow::Table> &table) {
    std::lock_guard guard(event_cache_mutex_);
    if (!event_cache_->exists(table.get())) {
        auto events = EventBatch::deserialize(table);
        // put it into cache
        event_cache_->put(table.get(), std::move(events));
    }
    return event_cache_->get(table.get());
}

std::shared_ptr<TransactionBatch> Loader::load_transactions(
    const std::shared_ptr<arrow::Table> &table) {
    std::lock_guard guard(transaction_cache_mutex_);
    if (!transaction_cache_->exists(table.get())) {
        auto transactions = TransactionBatch::deserialize(table);
        transaction_cache_->put(table.get(), std::move(transactions));
    }
    return transaction_cache_->get(table.get());
}

std::shared_ptr<TransactionGroupBatch> Loader::load_transaction_groups(
    const std::shared_ptr<arrow::Table> &table) {
    std::lock_guard guard(transaction_group_cache_mutex_);
    if (!transaction_group_cache_->exists(table.get())) {
        auto group = TransactionGroupBatch::deserialize(table);
        transaction_group_cache_->put(table.get(), std::move(group));
    }
    return transaction_group_cache_->get(table.get());
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
                break;
            }
            case FileInfo::FileType::transaction: {
                stats_.num_transaction_files++;
                stats_.num_transactions += table->num_rows();
                break;
            }
            case FileInfo::FileType::transaction_group: {
                stats_.num_transaction_group_files++;
                stats_.num_transaction_groups += table->num_rows();
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
            if constexpr (std::is_same<T, Event>::value) {
                if ((*values[i])[indices[i]]->time() < value->time()) {
                    min_index = i;
                    value = (*values[i])[indices[i]];
                }
            } else if constexpr (std::is_same<T, Transaction>::value ||
                                 std::is_same<T, TransactionGroup>::value) {
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
void load_values(
    const std::vector<LoaderResult> &load_results, std::vector<std::shared_ptr<T>> &values,
    const std::function<std::shared_ptr<T>(const std::shared_ptr<arrow::Table> &)> &load_func,
    std::unordered_set<arrow::Table *> &loaded_table_) {
    for (auto const &res : load_results) {
        auto const &table = res.table;
        if (loaded_table_.find(table.get()) != loaded_table_.end()) {
            // we already streamed this one
            continue;
        }
        loaded_table_.emplace(table.get());

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

    // we assume the event batch is sorted by time within
    std::unordered_set<arrow::Table *> loaded_table_;
    // we load them in approximation of time, it won't be perfect but should be good
    // enough
    uint64_t interval = (stats_.max_event_time - stats_.min_event_time) / stats_.num_event_files;
    for (uint64_t start_time = stats_.min_event_time; start_time <= stats_.max_event_time;
         start_time += interval) {
        auto load_results = load_events_table(start_time, start_time + interval);
        std::vector<std::shared_ptr<EventBatch>> events;
        std::vector<std::shared_ptr<TransactionBatch>> transactions;
        std::vector<std::shared_ptr<TransactionGroupBatch>> transaction_groups;

        // gcc failed to induce the template type
        load_values<EventBatch>(
            load_results, events,
            [this](const std::shared_ptr<arrow::Table> &table) { return load_events(table); },
            loaded_table_);

        // decide whether to stream transactions or not
        if (stream_transactions) {
            load_results = load_transaction_table(std::nullopt, start_time, start_time + interval);

            load_values<TransactionBatch>(
                load_results, transactions,
                [this](const std::shared_ptr<arrow::Table> &table) {
                    return load_transactions(table);
                },
                loaded_table_);

            // groups as well
            load_results =
                load_transaction_group_table(std::nullopt, start_time, start_time + interval);
            load_values<TransactionGroupBatch>(
                load_results, transaction_groups,
                [this](const std::shared_ptr<arrow::Table> &table) {
                    return load_transaction_groups(table);
                },
                loaded_table_);
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

}  // namespace hermes