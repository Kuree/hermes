#include "arrow.hh"

#include "arrow/api.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "parquet/stream_writer.h"

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

std::string get_string(const std::shared_ptr<arrow::Scalar> &scalar) {
    auto str_val_ = std::reinterpret_pointer_cast<arrow::StringScalar>(scalar);
    auto str_val = std::string(reinterpret_cast<const char *>(str_val_->value->data()));
    return str_val;
}

}  // namespace hermes