#include "serializer.hh"

#include <filesystem>
#include <fstream>

#include "arrow/api.h"
#include "arrow/io/file.h"
#include "arrow/ipc/reader.h"
#include "fmt/format.h"
#include "parquet/arrow/writer.h"
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
}

bool Serializer::serialize(const EventBatch &batch) {
    auto r = batch.validate();
    if (!r) return false;
    // serialize
    auto [record, schema] = batch.serialize();

    auto [parquet_name, json_name] = get_next_filename();
    auto res = serialize(parquet_name, record);
    if (!res) return false;

    // write out event batch properties
    write_stat(parquet_name, json_name, batch);
    return true;
}

bool Serializer::serialize(const TransactionBatch &batch) {
    // serialize
    auto [record, schema] = batch.serialize();

    auto [parquet_name, json_name] = get_next_filename();
    auto res = serialize(parquet_name, record);
    if (!res) return false;

    // write out event batch properties
    write_stat(parquet_name, json_name, batch);
    return true;
}

std::pair<std::string, std::string> Serializer::get_next_filename() {
    std::lock_guard guard(batch_mutex_);
    uint64_t id = batch_counter_++;
    auto parquet_name = fmt::format("{0}.parquet", id);
    auto json_name = fmt::format("{0}.json", id);
    return {parquet_name, json_name};
}

bool Serializer::serialize(const std::string &filename,
                           const std::shared_ptr<arrow::RecordBatch> &record) {
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    auto res_f = arrow::io::FileOutputStream::Open(filename);
    if (!res_f.ok()) return false;
    auto f = *res_f;

    // serialize
    auto table_r = arrow::Table::FromRecordBatches({record});
    if (!table_r.ok()) return false;
    auto table = *table_r;

    auto res =
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile,
                                   get_chunk_size(static_cast<uint64_t>(record->num_rows())));

    // write out table specifics

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

void write_stat_to_file(rapidjson::Document &document, const std::string &filename) {
    rapidjson::StringBuffer buffer;
    rapidjson::PrettyWriter w(buffer);
    document.Accept(w);
    auto const *s = buffer.GetString();

    std::ofstream file(filename, std::ios_base::trunc);
    file << s;
    file.close();
}

void Serializer::write_stat(const std::string &json_filename, const std::string &parquet_filename,
                            const EventBatch &batch) {
    rapidjson::Document document(rapidjson::kObjectType);
    set_member(document, "parquet", parquet_filename);

    // need to compute the max and min ID

    write_stat_to_file(document, json_filename);
}

void Serializer::write_stat(const std::string &json_filename, const std::string &parquet_filename,
                            const TransactionBatch &batch) {
    rapidjson::Document document(rapidjson::kObjectType);
    set_member(document, "parquet", parquet_filename);

    write_stat_to_file(document, json_filename);
}

}  // namespace hermes
