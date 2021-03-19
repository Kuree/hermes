#include "serializer.hh"

#include <filesystem>

#include "fmt/format.h"
#include "rapidjson/rapidjson.h"
#include "parquet/arrow/writer.h"
#include "arrow/io/file.h"
#include "arrow/api.h"
#include "arrow/ipc/reader.h"
#include "parquet/stream_writer.h"

namespace fs = std::filesystem;

namespace hermes {

Serializer::Serializer(std::string output_dir) : output_dir_(std::move(output_dir)) {
    if (!fs::exists(output_dir_)) {
        fs::create_directories(output_dir_);
    }
}

bool Serializer::serialize(const EventBatch &batch) {
    auto r = batch.validate();
    if (!r) return false;

    auto [parquet_name, json_name] = get_next_filename();
    // need to write out parquet
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    auto res_f = arrow::io::FileOutputStream::Open(parquet_name);
    if (!res_f.ok()) return false;
    auto f = *res_f;

    // serialize

}

bool Serializer::serialize(const TransactionBatch &batch) {}

std::pair<std::string, std::string> Serializer::get_next_filename() {
    std::lock_guard guard(batch_mutex_);
    uint64_t id = batch_counter_++;
    auto parquet_name = fmt::format("{0}.parquet", id);
    auto json_name = fmt::format("{0}.json", id);
    return {parquet_name, json_name};
}

}  // namespace hermes
