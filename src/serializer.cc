#include "serializer.hh"

#include <filesystem>
#include <fstream>
#include <utility>

#include "arrow/api.h"
#include "arrow/io/file.h"
#include "arrow/ipc/reader.h"
#include "fmt/format.h"
#include "loader.hh"
#include "parquet/arrow/writer.h"
#include "parquet/metadata.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "json.hh"

namespace fs = std::filesystem;

namespace hermes {

Serializer::Serializer(std::string output_dir) : Serializer(std::move(output_dir), true) {}

Serializer::Serializer(std::string output_dir, bool override) : output_dir_(std::move(output_dir)) {
    if (!fs::exists(output_dir_)) {
        fs::create_directories(output_dir_);
    }
    // we use version 2.0
    auto builder = parquet::WriterProperties::Builder();
    builder.version(parquet::ParquetVersion::PARQUET_2_0);
    // we use snappy as the compression scheme
    builder.compression(arrow::Compression::SNAPPY);
    // make sure to use statistic
    builder.enable_statistics();
    builder.max_statistics_size(1 << 20);
    writer_properties_ = builder.build();

    if (!override) {
        identify_batch_counter();
    }
}

bool Serializer::serialize(EventBatch &batch) {
    auto r = batch.validate();
    if (!r) return false;
    batch.sort();
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

bool Serializer::serialize(TransactionBatch &batch) {
    batch.sort();
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

bool Serializer::serialize(TransactionGroupBatch &batch) {
    batch.sort();
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

    return stat;
}

void Serializer::identify_batch_counter() {
    while (true) {
        auto json_name = fmt::format("{0}.json", batch_counter_);
        auto dir = fs::path(output_dir_);
        json_name = dir / json_name;
        if (fs::exists(json_name)) {
            batch_counter_++;
        } else {
            break;
        }
    }
}

bool Serializer::serialize(parquet::arrow::FileWriter *writer,
                           const std::shared_ptr<arrow::RecordBatch> &record) {
    if (!writer) return false;
    // serialize

    // we write out table by hand instead of using WriteTable
    auto r = writer->NewRowGroup(record->num_rows());
    if (!r.ok()) return false;
    for (int i = 0; i < record->num_columns(); i++) {
        auto const &column = record->column(i);
        r = writer->WriteColumnChunk(*column);
        if (!r.ok()) return false;
    }

    return true;
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
    if (stat.type.empty()) stat.type = "event";
    if (stat.name.empty() && !batch.name().empty()) {
        stat.name = batch.name();
    }
}

void Serializer::update_stat(SerializationStat &stat, const TransactionBatch &batch) {
    if (stat.type.empty()) stat.type = "transaction";
    if (stat.name.empty() && !batch.name().empty()) {
        stat.name = batch.name();
    }
}

void Serializer::update_stat(SerializationStat &stat, const TransactionGroupBatch &batch) {
    if (stat.type.empty()) stat.type = "transaction-group";
    if (stat.name.empty() && !batch.name().empty()) {
        stat.name = batch.name();
    }
}

void Serializer::write_stat(const SerializationStat &stat) {
    rapidjson::Document document(rapidjson::kObjectType);
    auto parquet_basename = std::string(fs::path(stat.parquet_filename).filename());
    json::set_member(document, "parquet", parquet_basename);
    json::set_member(document, "type", stat.type);
    json::set_member(document, "name", stat.name);

    write_stat_to_file(document, stat.json_filename);
}

}  // namespace hermes
