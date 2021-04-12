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

namespace fs = std::filesystem;

namespace hermes {

TransactionData TransactionDataIter::operator*() const {
    TransactionData data;
    if (current_row_ >= stream_->size()) {
        return data;
    }
    // need to load transactions
    // figure out which table to load
    auto table_iter = stream_->tables_.lower_bound(current_row_);
    auto const &table = table_iter->second;
    auto transactions = stream_->loader_->load_transactions(table);
    // compute the index
    auto offset = table_iter->first - current_row_;
    auto idx = table->num_rows() - offset;
    data.transaction = (*transactions)[idx];
    data.events = std::make_shared<EventBatch>();
    auto events = stream_->loader_->get_events(*data.transaction);
    data.events->reserve(events->size());
    for (auto const &e : *events) {
        data.events->emplace_back(e);
    }
    return data;
}

TransactionStream::TransactionStream(const std::vector<std::shared_ptr<arrow::Table>> &tables,
                                     Loader *loader)
    : loader_(loader) {
    num_entries_ = 0;
    for (auto const &table : tables) {
        num_entries_ += table->num_rows();
        tables_.emplace(num_entries_, table);
    }
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
    // compute stats. this should be very fast
    compute_stats();
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

std::vector<std::shared_ptr<TransactionBatch>> Loader::get_transactions(uint64_t min_time,
                                                                        uint64_t max_time) {
    // load the tables
    auto tables = load_transaction_table(std::nullopt, min_time, max_time);
    std::vector<std::shared_ptr<TransactionBatch>> result;
    result.reserve(tables.size());
    for (auto const &load_result : tables) {
        auto batch = load_transactions(load_result.table);
        batch->set_transaction_name(load_result.name);
        result.emplace_back(std::move(batch));
    }
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
            auto temp = result;
            result = std::make_shared<TransactionBatch>();
            result->reserve(temp->size() + batch->size());
            result->insert(result->end(), temp->begin(), temp->end());
            result->insert(result->end(), batch->begin(), batch->end());
            result->set_transaction_name(load_result.name);
        }
    }
    return result;
}

std::vector<std::shared_ptr<EventBatch>> Loader::get_events(uint64_t min_time, uint64_t max_time) {
    auto tables = load_events_table(min_time, max_time);
    std::vector<std::shared_ptr<EventBatch>> result;
    result.reserve(tables.size());
    for (auto const &load_result : tables) {
        auto batch = load_events(load_result.table);
        batch->set_event_name(load_result.name);
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
    auto tables = load_tables(files);
    std::shared_ptr<EventBatch> result;
    for (auto const &load_result : tables) {
        auto batch = load_events(load_result.table);
        if (!result) {
            result = batch;
            result->set_event_name(load_result.name);
        } else {
            // since the old one is used for caching
            // we have to create new result instead of reusing the old one
            auto temp = result;
            result = std::make_shared<EventBatch>();
            result->reserve(temp->size() + batch->size());
            result->insert(result->end(), temp->begin(), temp->end());
            result->insert(result->end(), batch->begin(), batch->end());
            result->set_event_name(load_result.name);
        }
    }

    return result;
}

inline std::unordered_map<const FileInfo *, std::vector<uint64_t>> compute_search_space(
    const std::vector<const FileInfo *> &events_, const Loader::FileMetadata &file_metadata_,
    const std::vector<uint64_t> &ids) {
    std::unordered_map<const FileInfo *, std::vector<uint64_t>> files;
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
    return files;
}

std::shared_ptr<EventBatch> Loader::get_events(const Transaction &transaction) {
    auto const &ids = transaction.events();
    auto files = compute_search_space(events_, file_metadata_, ids);

    std::unordered_map<uint64_t, Event *> id_mapping;
    for (auto const &[file, chunk_ids] : files) {
        for (auto const &chunk_id : chunk_ids) {
            auto table = tables_.at(std::make_pair(file, chunk_id));
            auto events = load_events(table);
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

    auto result = std::make_shared<EventBatch>();
    result->resize(transaction.events().size(), nullptr);
    for (auto i = 0; i < result->size(); i++) {
        auto const id = transaction.events()[i];
        if (id_mapping.find(id) != id_mapping.end()) {
            (*result)[i] = id_mapping.at(id)->shared_from_this();
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
    // need to gather all the tables given the info
    std::vector<std::shared_ptr<arrow::Table>> tables;
    for (auto const &[entry, table] : tables_) {
        auto const &[f, c_id] = entry;
        if (f == info) {
            tables.emplace_back(table);
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
    preload_table(info.get());

    files_.emplace_back(std::move(info));
}

bool Loader::preload_table(const FileInfo *file) {
    auto res_f = fs_->OpenInputFile(file->filename);
    if (!res_f.ok()) return false;
    auto f = *res_f;
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
    // TODO: implement cache replacement algorithm
    //   for now we hold all of them in memory
    std::lock_guard guard(event_cache_mutex_);
    if (event_cache_.find(table.get()) == event_cache_.end()) {
        auto events = EventBatch::deserialize(table);
        // put it into cache
        event_cache_.emplace(table.get(), std::move(events));
    }
    return event_cache_.at(table.get());
}

std::shared_ptr<TransactionBatch> Loader::load_transactions(
    const std::shared_ptr<arrow::Table> &table) {
    std::lock_guard guard(transaction_cache_mutex_);
    if (transaction_cache_.find(table.get()) == transaction_cache_.end()) {
        auto transactions = TransactionBatch::deserialize(table);
        transaction_cache_.emplace(table.get(), std::move(transactions));
    }
    return transaction_cache_.at(table.get());
}

void Loader::compute_stats() {
    for (auto const &[info, table] : tables_) {
        auto const &[file, blk_id] = info;
        if (file->type == FileInfo::FileType::event) {
            stats_.num_event_files++;
            stats_.num_events += table->num_rows();
            // need to load the column stats

            auto const &stats = file_metadata_.at(file).at("time")[blk_id];
            auto typed =
                std::reinterpret_pointer_cast<parquet::TypedStatistics<arrow::UInt64Type>>(stats);
            auto min = typed->min();
            auto max = typed->max();

            if (min < stats_.min_event_time) {
                stats_.min_event_time = min;
            }

            if (max > stats_.max_event_time) {
                stats_.max_event_time = max;
            }

        } else {
            stats_.num_transaction_files++;
            stats_.num_transactions += table->num_rows();
        }
    }
}

void Loader::stream(bool stream_transactions) {
    stream(MessageBus::default_bus(), stream_transactions);
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
        for (auto const &res : load_results) {
            auto const &table = res.table;
            if (loaded_table_.find(table.get()) != loaded_table_.end()) {
                // we already streamed this one
                continue;
            }
            loaded_table_.emplace(table.get());
            // stream this event out
            auto event_batch = load_events(table);
            event_batch->set_event_name(res.name);
            events.emplace_back(std::move(event_batch));
        }

        // decide whether to stream transactions or not
        if (stream_transactions) {
            load_results = load_transaction_table(std::nullopt, start_time, start_time + interval);
            for (auto const &res : load_results) {
                auto const &table = res.table;
                if (loaded_table_.find(table.get()) != loaded_table_.end()) {
                    // we already streamed this one
                    continue;
                }
                loaded_table_.emplace(table.get());
                // stream this event out
                auto transaction_batch = load_transactions(table);
                transaction_batch->set_transaction_name(res.name);
                transactions.emplace_back(std::move(transaction_batch));
            }
        }

        std::vector<uint64_t> event_indices;
        event_indices.resize(events.size(), 0);
        std::vector<uint64_t> transaction_indices;
        transaction_indices.resize(transactions.size(), 0);

        // stream out the events and transactions

        while (true) {
            bool stop = true;
            if (!events.empty()) {
                for (uint64_t i = 0; i < events.size(); i++) {
                    if (event_indices[i] < events[i]->size()) {
                        stop = false;
                    }
                }
            }
            if (!transactions.empty()) {
                for (uint64_t i = 0; i < transactions.size(); i++) {
                    if (transaction_indices[i] < transactions[i]->size()) {
                        stop = false;
                    }
                }
            }
            if (stop) break;

            // search for the min index
            if (!events.empty()) {
                uint32_t min_index = 0;
                auto event = (*events[min_index])[event_indices[min_index]];
                for (uint64_t i = 1; i < events.size(); i++) {
                    if ((*events[i])[event_indices[i]]->time() < event->time()) {
                        min_index = i;
                        event = (*events[i])[event_indices[i]];
                    }
                }
                event_indices[min_index]++;
                bus->publish(events[min_index]->event_name(), event);

                // need to be careful about the boundary
                if (event_indices[min_index] >= events[min_index]->size()) {
                    // need to pop that entry
                    events.erase(events.begin() + min_index);
                    event_indices.erase(event_indices.begin() + min_index);
                }
            }

            if (!transactions.empty()) {
                uint32_t min_index = 0;
                auto transaction = (*transactions[min_index])[transaction_indices[min_index]];
                for (uint64_t i = 1; i < events.size(); i++) {
                    if ((*transactions[i])[transaction_indices[i]]->start_time() <
                        transaction->start_time()) {
                        min_index = i;
                        transaction = (*transactions[i])[transaction_indices[i]];
                    }
                }
                transaction_indices[min_index]++;
                bus->publish(events[min_index]->event_name(), transaction);

                // need to be careful about the boundary
                if (transaction_indices[min_index] >= transactions[min_index]->size()) {
                    // need to pop that entry
                    transactions.erase(transactions.begin() + min_index);
                    transaction_indices.erase(transaction_indices.begin() + min_index);
                }
            }
        }
    }
}

std::vector<LoaderResult> Loader::load_events_table(uint64_t min_time, uint64_t max_time) {
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
    return tables;
}

std::vector<LoaderResult> Loader::load_transaction_table(const std::optional<std::string> &name,
                                                         uint64_t min_time, uint64_t max_time) {
    std::vector<std::pair<const FileInfo *, std::vector<uint64_t>>> files;
    for (auto const &file : transactions_) {
        if (name) {
            if (file->name != name) continue;
        }
        auto stats = file_metadata_.at(file);
        // we use time column
        auto max_time_stats = stats.at("end_time");
        auto min_time_stats = stats.at("start_time");
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
    return tables;
}

}  // namespace hermes