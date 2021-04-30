#include "arrow.hh"

#include <filesystem>
#include <iostream>

#include "arrow/api.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/util/uri.h"
#include "transaction.hh"
#include "fmt/format.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"

namespace hermes {

std::shared_ptr<arrow::Buffer> allocate_buffer(uint64_t size) {
    auto r = arrow::AllocateBuffer(static_cast<int64_t>(size));
    if (!r.ok()) {
        return nullptr;
    } else {
        std::shared_ptr<arrow::Buffer> ptr = std::move(*r);
        return ptr;
    }
}

std::shared_ptr<arrow::Buffer> serialize(const std::shared_ptr<arrow::RecordBatch> &batch,
                                         const std::shared_ptr<arrow::Schema> &schema) {
    auto mock_sink = arrow::io::MockOutputStream();
    auto stream_writer_r = arrow::ipc::MakeStreamWriter(&mock_sink, schema);
    if (!stream_writer_r.ok()) return nullptr;
    auto &stream_writer = *stream_writer_r;
    (void)stream_writer->WriteRecordBatch(*batch);
    (void)stream_writer->Close();
    auto data_size = *mock_sink.Tell();
    auto buff = allocate_buffer(data_size);
    // TODO: refactor the code later if we want to save the data
    arrow::io::FixedSizeBufferWriter stream(buff);
    auto writer = arrow::ipc::MakeStreamWriter(&stream, schema);
    if (!writer.ok()) return nullptr;
    (void)(*writer)->WriteRecordBatch(*batch);
    (void)(*writer)->Close();
    return buff;
}

void get_schema(const std::map<std::string, AttributeValue> &values,
                std::vector<std::shared_ptr<arrow::Field>> &fields) {
    for (auto const &[name, v] : values) {
        std::shared_ptr<arrow::DataType> type;
        std::visit(overloaded{[&type](uint8_t) { type = arrow::uint8(); },
                              [&type](uint16_t) { type = arrow::uint16(); },
                              [&type](uint32_t) { type = arrow::uint32(); },
                              [&type](uint64_t) { type = arrow::uint64(); },
                              [&type](bool) { type = arrow::boolean(); },
                              [&type](const std::string &) { type = arrow::utf8(); }},
                   v);
        auto field = std::make_shared<arrow::Field>(name, type);
        fields.emplace_back(field);
    }
}


template <typename T>
void serialize(const T *batch, std::vector<std::shared_ptr<arrow::Array>> &arrays) {
    auto *pool = arrow::default_memory_pool();
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    // initialize the builders based on the index sequence
    {
        auto const &event = *(*batch)[0];
        for (auto const &[name, v] : event.values()) {
            // visit the variant
            std::visit(
                overloaded{[pool, &builders](uint8_t) {
                  builders.emplace_back(std::make_unique<arrow::UInt8Builder>(pool));
                },
                           [pool, &builders](uint16_t) {
                             builders.emplace_back(std::make_unique<arrow::UInt16Builder>(pool));
                           },
                           [pool, &builders](uint32_t) {
                             builders.emplace_back(std::make_unique<arrow::UInt32Builder>(pool));
                           },
                           [pool, &builders](uint64_t) {
                             builders.emplace_back(std::make_unique<arrow::UInt64Builder>(pool));
                           },
                           [pool, &builders](bool) {
                             builders.emplace_back(std::make_unique<arrow::BooleanBuilder>(pool));
                           },
                           [pool, &builders](const std::string &) {
                             builders.emplace_back(std::make_unique<arrow::StringBuilder>(pool));
                           }},
                v);
        }
    }
    // write each row
    for (auto const &event : *batch) {
        auto const &values = event->values();
        uint64_t idx = 0;
        for (auto const &[name, value] : values) {
            auto *ptr = builders[idx++].get();
            // visit the variant
            std::visit(overloaded{[ptr](uint8_t arg) {
                         auto *p = reinterpret_cast<arrow::UInt8Builder *>(ptr);
                         (void)p->Append(arg);
                       },
                                  [ptr](uint16_t arg) {
                                    auto *p = reinterpret_cast<arrow::UInt16Builder *>(ptr);
                                    (void)p->Append(arg);
                                  },
                                  [ptr](uint32_t arg) {
                                    auto *p = reinterpret_cast<arrow::UInt32Builder *>(ptr);
                                    (void)p->Append(arg);
                                  },
                                  [ptr](uint64_t arg) {
                                    auto *p = reinterpret_cast<arrow::UInt64Builder *>(ptr);
                                    (void)p->Append(arg);
                                  },
                                  [ptr](bool arg) {
                                    auto *p = reinterpret_cast<arrow::BooleanBuilder *>(ptr);
                                    (void)p->Append(arg);
                                  },
                                  [ptr](const std::string &arg) {
                                    auto *p = reinterpret_cast<arrow::StringBuilder *>(ptr);
                                    (void)p->Append(arg);
                                  }},

                       value);
        }
    }
    // arrays
    arrays.reserve(builders.size());
    for (auto &builder : builders) {
        std::shared_ptr<arrow::Array> array;
        (void)builder->Finish(&array);
        arrays.emplace_back(array);
    }
}

void serialize(const EventBatch *batch, std::vector<std::shared_ptr<arrow::Field>> &fields,
               std::vector<std::shared_ptr<arrow::Array>> &arrays) {
    get_schema(batch->front()->values(), fields);
    serialize(batch, arrays);
}

void serialize(const TransactionBatch *batch, std::vector<std::shared_ptr<arrow::Field>> &fields,
               std::vector<std::shared_ptr<arrow::Array>> &arrays) {
    get_schema(batch->front()->values(), fields);
    serialize(batch, arrays);
}

std::shared_ptr<arrow::RecordBatch> get_batch(const std::shared_ptr<arrow::Buffer> &buffer) {
    auto buff = arrow::io::BufferReader(buffer);
    auto reader_r = arrow::ipc::RecordBatchStreamReader::Open(&buff);
    if (!reader_r.ok()) return nullptr;
    auto &reader = *reader_r;

    std::shared_ptr<arrow::RecordBatch> batch;
    auto r = reader->ReadNext(&batch);
    if (!r.ok()) return nullptr;
    return batch;
}

std::shared_ptr<arrow::Table> deserialize(const std::shared_ptr<arrow::Buffer> &buffer) {
    auto batch = get_batch(buffer);
    if (!batch) return nullptr;
    auto table = arrow::Table::FromRecordBatches({batch});
    if (!table.ok()) return nullptr;
    return *table;
}

std::shared_ptr<arrow::Table> load_table(const std::string &filename) {
    auto *pool = arrow::default_memory_pool();
    // the arrow cpp documentation is so bad that it seems impossible to create a RandomAccessFile
    // since there is no function actually produce a random access file
    // https://arrow.apache.org/docs/cpp/parquet.html

    auto reader = parquet::ParquetFileReader::OpenFile(filename);
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    auto r = parquet::arrow::FileReader::Make(pool, std::move(reader), &arrow_reader);
    if (!r.ok()) {
        return nullptr;
    }
    std::shared_ptr<arrow::Table> table;
    r = arrow_reader->ReadTable(&table);
    if (!r.ok()) return nullptr;
    return table;
}

uint8_t get_uint8(const std::shared_ptr<arrow::Scalar> &scalar) {
    auto int_val_ = std::reinterpret_pointer_cast<arrow::UInt8Scalar>(scalar);
    return int_val_->value;
}

uint16_t get_uint16(const std::shared_ptr<arrow::Scalar> &scalar) {
    auto int_val_ = std::reinterpret_pointer_cast<arrow::UInt16Scalar>(scalar);
    return int_val_->value;
}

uint32_t get_uint32(const std::shared_ptr<arrow::Scalar> &scalar) {
    auto int_val_ = std::reinterpret_pointer_cast<arrow::UInt32Scalar>(scalar);
    return int_val_->value;
}

uint64_t get_uint64(const std::shared_ptr<arrow::Scalar> &scalar) {
    auto int_val_ = std::reinterpret_pointer_cast<arrow::UInt64Scalar>(scalar);
    return int_val_->value;
}

int16_t get_int16(const std::shared_ptr<arrow::Scalar> &scalar) {
    auto int_val_ = std::reinterpret_pointer_cast<arrow::Int16Scalar>(scalar);
    return int_val_->value;
}

int32_t get_int32(const std::shared_ptr<arrow::Scalar> &scalar) {
    auto int_val_ = std::reinterpret_pointer_cast<arrow::Int32Scalar>(scalar);
    return int_val_->value;
}

int64_t get_int64(const std::shared_ptr<arrow::Scalar> &scalar) {
    auto int_val_ = std::reinterpret_pointer_cast<arrow::Int64Scalar>(scalar);
    return int_val_->value;
}

std::string get_string(const std::shared_ptr<arrow::Scalar> &scalar) {
    auto str_val_ = std::reinterpret_pointer_cast<arrow::StringScalar>(scalar);
    auto str_val = std::string(reinterpret_cast<const char *>(str_val_->value->data()));
    return str_val;
}

bool get_bool(const std::shared_ptr<arrow::Scalar> &scalar) {
    auto bool_val_ = std::reinterpret_pointer_cast<arrow::BooleanScalar>(scalar);
    return bool_val_->value;
}

std::vector<uint64_t> get_uint64s(const std::shared_ptr<arrow::Scalar> &scalar) {
    auto list_scalar = std::reinterpret_pointer_cast<arrow::ListScalar>(scalar);
    auto size = list_scalar->value->length();
    std::vector<uint64_t> result(size);
    for (int64_t j = 0; j < result.size(); j++) {
        auto v = get_uint64(*list_scalar->value->GetScalar(j));
        result[j] = v;
    }
    return result;
}

std::vector<bool> get_bools(const std::shared_ptr<arrow::Scalar> &scalar) {
    auto list_scalar = std::reinterpret_pointer_cast<arrow::ListScalar>(scalar);
    auto size = list_scalar->value->length();
    std::vector<bool> result(size);
    for (int64_t j = 0; j < result.size(); j++) {
        auto v = get_bool(*list_scalar->value->GetScalar(j));
        result[j] = v;
    }
    return result;
}

FileSystemInfo::FileSystemInfo(const std::string &path) {
    if (path.find("://") == std::string::npos) {
        this->path = std::filesystem::absolute(path);
    } else {
        arrow::internal::Uri uri;
        auto res = uri.Parse(path);
        if (!res.ok() || uri.scheme() != "s3") {
            throw std::runtime_error("Invalid path " + path);
        }

        this->path = uri.host() + uri.path();

        is_s3_ = true;
    }
}

void FileSystemInfo::clear() const {
    auto fs = load_fs(*this);
    if (!fs) return;
    // need to clear out the directory
    (void)fs->DeleteDir(path);
}

std::shared_ptr<arrow::fs::FileSystem> load_fs(const FileSystemInfo &info) {
    static std::unordered_map<std::string, std::shared_ptr<arrow::fs::FileSystem>> fs_map_;
    std::shared_ptr<arrow::fs::FileSystem> result;
    if (fs_map_.find(info.path) != fs_map_.end()) {
        return fs_map_.at(info.path);
    }

    if (info.is_s3()) {
        // construct s3 filesystem
        arrow::fs::S3Options options;
        if (!info.access_key.empty()) {
            options = arrow::fs::S3Options::FromAccessKey(info.access_key, info.secret_key);
        }
        if (!info.end_point.empty()) {
            options.endpoint_override = info.end_point;
        }
        // we hardcoded regions for now
        options.region = "us-west-2";
        auto res = arrow::fs::InitializeS3({arrow::fs::S3LogLevel::Fatal});
        if (!res.ok()) return nullptr;
        auto fs_res = arrow::fs::S3FileSystem::Make(options);
        if (!fs_res.ok()) {
            // need to print out the filesystem error since it's critical
            std::cerr << "[ERROR]: " << fs_res.status().ToString() << std::endl;
            return nullptr;
        } else {
            result = *fs_res;
        }
    } else {
        result = std::make_shared<arrow::fs::LocalFileSystem>();
    }
    fs_map_.emplace(info.path, result);
    return result;
}

}  // namespace hermes