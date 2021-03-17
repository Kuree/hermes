#include "event.hh"

#include <type_traits>
#include <unordered_map>
#include <variant>

#include "arrow/api.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "parquet/stream_writer.h"
#include "plasma/client.h"

namespace hermes {

static std::unordered_map<std::size_t, std::shared_ptr<arrow::DataType>> types_;

void init_type_mapping() {
    if (!types_.empty()) return;
#define ADD_ELEMENT(x) types_.emplace(typeid(x##_t).hash_code(), arrow::x())
    ADD_ELEMENT(uint8);
    ADD_ELEMENT(uint16);
    ADD_ELEMENT(uint32);
    ADD_ELEMENT(uint64);
#undef ADD_ELEMENT
    // string is special
    types_.emplace(typeid(std::string).hash_code(), arrow::utf8());
}

template <class V>
std::type_info const &var_type(V const &v) {
    return std::visit([](auto &&x) -> decltype(auto) { return typeid(x); }, v);
}

std::shared_ptr<arrow::Schema> get_schema(const Event &event) {
    auto const &values = event.values();
    init_type_mapping();
    std::vector<std::shared_ptr<arrow::Field>> schema_vector;

    for (auto const &[name, v] : values) {
        const auto &type = var_type(v);
        auto filed = std::make_shared<arrow::Field>(name, types_.at(type.hash_code()));
    }
    // time is last
    schema_vector.emplace_back(std::make_shared<arrow::Field>("time", arrow::uint64()));

    return std::make_shared<arrow::Schema>(schema_vector);
}

template <class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

// TODO: better handling
bool EventBatch::serialize(
    const std::function<const std::shared_ptr<arrow::Buffer> &(uint64_t)> &buffer_allocator) {
    if (empty()) return false;
    // we assume it is already validated
    std::shared_ptr<arrow::Schema> schema;
    {
        auto const &event = *(*this)[0];
        schema = get_schema(event);
    }
    // we need to initialize the type builder
    auto *pool = arrow::default_memory_pool();
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    auto time_builder = std::make_unique<arrow::UInt64Builder>(pool);
    // initialize the builders based on the index sequence
    {
        auto const &event = *(*this)[0];
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
                               builders.emplace_back(std::make_unique<arrow::UInt16Builder>(pool));
                           },
                           [pool, &builders](uint64_t) {
                               builders.emplace_back(std::make_unique<arrow::UInt16Builder>(pool));
                           },
                           [pool, &builders](const std::string &) {
                               builders.emplace_back(std::make_unique<arrow::StringBuilder>(pool));
                           }},
                v);
        }
    }
    // write each row
    for (auto const &event : *this) {
        (void)time_builder->Append(event->time());
        auto const &values = event->values();
        for (auto const &[name, value] : values) {
            auto *ptr = builders[value.index()].get();
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
                                  [ptr](const std::string &arg) {
                                      auto *p = reinterpret_cast<arrow::StringBuilder *>(ptr);
                                      (void)p->Append(arg);
                                  }},
                       value);
        }
    }
    // arrays
    builders.emplace_back(std::move(time_builder));
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(builders.size());
    for (auto &builder : builders) {
        std::shared_ptr<arrow::Array> array;
        (void)builder->Finish(&array);
        arrays.emplace_back(array);
    }

    auto batch = arrow::RecordBatch::Make(schema, size(), arrays);
    auto mock_sink = arrow::io::MockOutputStream();
    auto stream_writer_r = arrow::ipc::MakeStreamWriter(&mock_sink, schema);
    if (!stream_writer_r.ok()) return false;
    auto &stream_writer = *stream_writer_r;
    (void)stream_writer->WriteRecordBatch(*batch);
    (void)stream_writer->Close();
    auto data_size = *mock_sink.Tell();
    auto buff = buffer_allocator(data_size);
    // TODO: refactor the code later if we want to save the data
    arrow::io::FixedSizeBufferWriter stream(buff);
    auto writer = arrow::ipc::MakeStreamWriter(&stream, schema);
    if (!writer.ok()) return false;
    (void)(*writer)->WriteRecordBatch(*batch);
    (void)(*writer)->Close();
    return true;
}

std::unique_ptr<EventBatch> EventBatch::deserialize(const std::shared_ptr<arrow::Buffer> &buffer) {
    auto buff = arrow::io::BufferReader(buffer);
    auto reader_r = arrow::ipc::RecordBatchStreamReader::Open(&buff);
    if (!reader_r.ok()) return nullptr;
    auto &reader = *reader_r;

    std::shared_ptr<arrow::RecordBatch> batch;
    auto r = reader->ReadNext(&batch);
    if (!r.ok()) return nullptr;

    // construct the event batch
    auto event_batch = std::make_unique<EventBatch>("");
    event_batch->reserve(batch->num_rows());
    // create each batch
    for (auto i = 0; i < batch->num_rows(); i++) {
        event_batch->emplace_back(std::make_unique<Event>(0));
    }

    auto uint8 = arrow::uint8();
    auto uint16 = arrow::uint16();
    auto uint32 = arrow::uint32();
    auto uint64 = arrow::uint64();
    auto str_ = arrow::utf8();

    // look through each column and fill in data
    auto const &schema = batch->schema();
    for (int i = 0; i < batch->num_columns(); i++) {
        auto const &name = batch->column_name(i);
        auto type = schema->field(i)->type();
        auto const &column = batch->column(i);
        for (int j = 0; j < batch->num_rows(); j++) {
            auto r_ = column->GetScalar(j);
            if (!r_.ok()) return nullptr;
            auto const &v = *r_;
            if (type->Equals(str_)) {
                auto str_val_ = std::reinterpret_pointer_cast<arrow::StringScalar>(v);
                auto str_val = std::string(reinterpret_cast<const char *>(str_val_->value->data()));
                (*event_batch)[j]->add_value(name, str_val);
            } else if (type->Equals(uint8)) {
                auto int_val_ = std::reinterpret_pointer_cast<arrow::UInt8Scalar>(v);
                (*event_batch)[j]->add_value(name, int_val_->value);
            } else if (type->Equals(uint16)) {
                auto int_val_ = std::reinterpret_pointer_cast<arrow::UInt16Scalar>(v);
                (*event_batch)[j]->add_value(name, int_val_->value);
            } else if (type->Equals(uint32)) {
                auto int_val_ = std::reinterpret_pointer_cast<arrow::UInt32Scalar>(v);
                (*event_batch)[j]->add_value(name, int_val_->value);
            } else if (type->Equals(uint64)) {
                auto int_val_ = std::reinterpret_pointer_cast<arrow::UInt64Scalar>(v);
                if (name == "time") {
                    (*event_batch)[j]->set_time(int_val_->value);
                } else {
                    (*event_batch)[j]->add_value(name, int_val_->value);
                }
            } else {
                throw std::runtime_error("Unknown type " + type->ToString());
            }
        }
    }

    return std::move(event_batch);
}

bool EventBatch::validate() const noexcept {
    // make sure they have the same schema
    if (empty()) return true;
    auto const &ref = this->front()->values();
    for (uint64_t i = 1; i < size(); i++) {
        auto const &event = (*this)[i];
        auto const &values = event->values();
        if (values.size() != ref.size()) return false;
        for (auto const &[name, v] : ref) {
            if (values.find(name) == values.end()) {
                return false;
            }
            if (v.index() != values.at(name).index()) return false;
        }
    }
    return true;
}

}  // namespace hermes