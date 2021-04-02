#include "serializer.hh"

#include <filesystem>
#include <fstream>

#include "arrow/api.h"
#include "arrow/io/file.h"
#include "arrow/ipc/reader.h"
#include "fmt/format.h"
#include "loader.hh"
#include "parquet/arrow/writer.h"
#include "parquet/metadata.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace fs = std::filesystem;

constexpr auto num_chunks = 16;

namespace hermes {

uint64_t get_chunk_size(uint64_t num_rows) { return std::max<uint64_t>(1, num_rows / num_chunks); }

Serializer::Serializer(std::string output_dir) : output_dir_(std::move(output_dir)) {
    if (!fs::exists(output_dir_)) {
        fs::create_directories(output_dir_);
    }
    // we use version 2.0
    auto builder = parquet::WriterProperties::Builder();
    builder.version(parquet::ParquetVersion::PARQUET_2_0);
    // we use snappy as the compression scheme
    builder.compression(arrow::Compression::SNAPPY);
    writer_properties_ = builder.build();
}

bool Serializer::serialize(const EventBatch &batch) {
    auto r = batch.validate();
    if (!r) return false;
    // serialize
    auto [record, schema] = batch.serialize();
    auto *writer = get_writer(&batch, schema);
    auto res = serialize(writer, record);
    if (!res) return false;

    // write out event batch properties
    auto &stat = get_stat(&batch);
    update_stat(stat, batch);

    return true;
}

bool Serializer::serialize(const TransactionBatch &batch) {
    // serialize
    auto [record, schema] = batch.serialize();
    auto *writer = get_writer(&batch, schema);
    auto res = serialize(writer, record);
    if (!res) return false;

    // write out event batch properties
    auto &stat = get_stat(&batch);
    update_stat(stat, batch);
    return true;
}

void Serializer::finalize() {
    if (writers_.empty()) return;
    for (auto const &[ptr, writer] : writers_) {
        (void)writer->Close();
    }
    for (auto const &[ptr, stat] : stats_) {
        write_stat(stat);
    }

    writers_.clear();
    stats_.clear();
}

std::pair<std::string, std::string> Serializer::get_next_filename() {
    std::lock_guard guard(batch_mutex_);
    uint64_t id = batch_counter_++;
    auto parquet_name = fmt::format("{0}.parquet", id);
    auto json_name = fmt::format("{0}.json", id);
    auto dir = fs::path(output_dir_);
    parquet_name = dir / parquet_name;
    json_name = dir / json_name;
    return {parquet_name, json_name};
}

parquet::arrow::FileWriter *Serializer::get_writer(const void *ptr,
                                                   const std::shared_ptr<arrow::Schema> &schema) {
    if (writers_.find(ptr) != writers_.end()) {
        return writers_.at(ptr).get();
    } else {
        // need to create a new set of files
        auto &stat = get_stat(ptr);
        auto res_f = arrow::io::FileOutputStream::Open(stat.parquet_filename);
        if (!res_f.ok()) return nullptr;
        auto out_file = *res_f;

        std::unique_ptr<parquet::arrow::FileWriter> writer;
        auto res = parquet::arrow::FileWriter::Open(
            *schema, arrow::default_memory_pool(), std::move(out_file), writer_properties_,
            parquet::default_arrow_writer_properties(), &writer);
        if (!res.ok()) return nullptr;
        // transfer ownership
        std::shared_ptr<parquet::arrow::FileWriter> writer_ptr = std::move(writer);
        writers_.emplace(ptr, writer_ptr);
        return writer_ptr.get();
    }
}

SerializationStat &Serializer::get_stat(const void *ptr) {
    if (stats_.find(ptr) != stats_.end()) {
        return stats_.at(ptr);
    }

    auto [parquet_name, json_name] = get_next_filename();
    auto &stat = stats_[ptr];
    stat.parquet_filename = parquet_name;
    stat.json_filename = json_name;
    stat.max_id = 0;
    stat.max_time = 0;
    stat.min_id = std::numeric_limits<uint64_t>::max();
    stat.min_time = std::numeric_limits<uint64_t>::max();

    return stat;
}

bool Serializer::serialize(parquet::arrow::FileWriter *writer,
                           const std::shared_ptr<arrow::RecordBatch> &record) {
    if (!writer) return false;
    // serialize
    auto table_r = arrow::Table::FromRecordBatches({record});
    if (!table_r.ok()) return false;
    auto table = *table_r;

    // write the table
    uint64_t chunk_size = get_chunk_size(static_cast<uint64_t>(record->num_rows()));
    auto res = writer->WriteTable(*table, chunk_size);
    return res.ok();
}

// the implementation below copies from hgdb
template <typename T, typename K, typename A>
void set_member(K &json_value, A &allocator, const char *name, const T &value) {
    rapidjson::Value key(name, allocator);  // NOLINT
    if constexpr (std::is_same<T, std::string>::value) {
        rapidjson::Value v(value.c_str(), allocator);
        json_value.AddMember(key, v, allocator);
    } else if constexpr (std::is_integral<T>::value) {
        json_value.AddMember(key.Move(), value, allocator);
    } else if constexpr (std::is_same<T, rapidjson::Value>::value) {
        rapidjson::Value v_copy(value, allocator);
        json_value.AddMember(key.Move(), v_copy.Move(), allocator);
    } else if constexpr (std::is_same<T, std::map<std::string, std::string>>::value) {
        rapidjson::Value v(rapidjson::kObjectType);
        for (auto const &[name, value_] : value) {
            set_member(v, allocator, name.c_str(), value_);
        }
        json_value.AddMember(key.Move(), v.Move(), allocator);
    } else {
        throw std::runtime_error(fmt::format("Unable type for {0}", name));
    }
}

template <typename T>
void set_member(rapidjson::Document &document, const char *name, const T &value) {
    auto &allocator = document.GetAllocator();
    set_member(document, allocator, name, value);
}

void set_member(rapidjson::Document &document, const char *name, const char *value) {
    auto &allocator = document.GetAllocator();
    rapidjson::Value key(name, allocator);  // NOLINT
    rapidjson::Value v(value, allocator);
    document.AddMember(key, v, allocator);
}

void write_stat_to_file(rapidjson::Document &document, const std::string &filename) {
    rapidjson::StringBuffer buffer;
    rapidjson::PrettyWriter w(buffer);
    document.Accept(w);
    auto const *s = buffer.GetString();

    std::ofstream file(filename, std::ios_base::trunc);
    file << s;
    file.close();
}

void Serializer::update_stat(SerializationStat &stat, const EventBatch &batch) {
    for (auto const &event : batch) {
        if (event->time() > stat.max_time) stat.max_time = event->time();
        if (event->time() < stat.min_time) stat.min_time = event->time();
        stat.max_id = std::max(stat.max_id, event->id());
        stat.min_id = std::min(stat.min_id, event->id());
    }
    if (stat.type.empty()) stat.type = "event";
    if (stat.name.empty() && !batch.event_name().empty()) {
        stat.name = batch.event_name();
    }
}

void Serializer::update_stat(SerializationStat &stat, const TransactionBatch &batch) {
    for (auto const &transaction : batch) {
        if (transaction->end_time() > stat.max_time) stat.max_time = transaction->end_time();
        if (transaction->start_time() < stat.min_time) stat.min_time = transaction->start_time();
        stat.max_id = std::max(stat.max_id, transaction->id());
        stat.min_id = std::min(stat.min_id, transaction->id());
    }
    if (stat.type.empty()) stat.type = "transaction";
    if (stat.name.empty() && !batch.transaction_name().empty()) {
        stat.name = batch.transaction_name();
    }
}

void Serializer::write_stat(const SerializationStat &stat) {
    rapidjson::Document document(rapidjson::kObjectType);
    auto parquet_basename = std::string(fs::path(stat.parquet_filename).filename());
    set_member(document, "parquet", parquet_basename);
    set_member(document, "type", stat.type);
    set_member(document, "name", stat.name);

    set_member(document, "min_time", stat.min_time);
    set_member(document, "max_time", stat.max_time);
    set_member(document, "min_id", stat.min_id);
    set_member(document, "max_id", stat.max_id);

    write_stat_to_file(document, stat.json_filename);
}

}  // namespace hermes
