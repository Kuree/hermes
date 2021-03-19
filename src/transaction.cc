#include "transaction.hh"

#include "arrow.hh"
#include "arrow/api.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "parquet/stream_writer.h"

namespace hermes {

bool Transaction::add_event(const std::unique_ptr<Event> &event) {
    if (is_done_) return false;
    if (start_time_ > event->time()) {
        start_time_ = event->time();
    }
    if (end_time_ < event->time()) {
        end_time_ = event->time();
    }

    events_ids_.emplace_back(event->id());
    return true;
}

std::shared_ptr<arrow::Buffer> TransactionBatch::serialize(
    const std::function<std::shared_ptr<arrow::Buffer>(uint64_t)> &buffer_allocator) {
    // we need to serialize a list of event ids
    auto const &list = *this;
    auto *pool = arrow::default_memory_pool();
    arrow::UInt64Builder id_builder(pool);
    arrow::UInt64Builder start_builder(pool);
    arrow::UInt64Builder end_builder(pool);
    arrow::ListBuilder event_ids_list_builder(pool, std::make_shared<arrow::UInt64Builder>(pool));
    auto &event_ids_builder =
        *(reinterpret_cast<arrow::UInt64Builder *>(event_ids_list_builder.value_builder()));

    for (auto const &transaction : list) {
        (void)id_builder.Append(transaction->id());
        (void)start_builder.Append(transaction->start_time());
        (void)end_builder.Append(transaction->end_time());
        std::vector<uint64_t> ids;
        auto const &events = transaction->events();
        ids.reserve(events.size());
        for (auto const &id : events) ids.emplace_back(id);
        // indicate the start of a new list row
        (void)event_ids_list_builder.Append();
        (void)event_ids_builder.AppendValues(ids.data(), ids.size());
    }
    std::shared_ptr<arrow::Array> id_array;
    auto r = id_builder.Finish(&id_array);
    if (!r.ok()) return nullptr;
    std::shared_ptr<arrow::Array> start_array;
    r = start_builder.Finish(&start_array);
    if (!r.ok()) return nullptr;
    std::shared_ptr<arrow::Array> end_array;
    r = end_builder.Finish(&end_array);
    if (!r.ok()) return nullptr;
    std::shared_ptr<arrow::Array> event_id_array;
    r = event_ids_list_builder.Finish(&event_id_array);
    if (!r.ok()) return nullptr;

    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
        arrow::field("id", arrow::uint64()), arrow::field("start_time", arrow::uint64()),
        arrow::field("end_time", arrow::uint64()),
        arrow::field("events", arrow::list(arrow::uint64()))};
    auto schema = std::make_shared<arrow::Schema>(schema_vector);

    auto batch = arrow::RecordBatch::Make(schema, size(),
                                          {id_array, start_array, end_array, event_id_array});
    return ::hermes::serialize(batch, schema, buffer_allocator);
}

std::unique_ptr<TransactionBatch> TransactionBatch::deserialize(
    const std::shared_ptr<arrow::Buffer> &buffer) {
    auto batch = get_batch(buffer);
    if (!batch) return nullptr;

    auto id_column = batch->column(0);
    auto start_time = batch->column(1);
    auto end_time = batch->column(2);
    auto events = batch->column(3);

    auto transactions = std::make_unique<TransactionBatch>();
    transactions->reserve(batch->num_rows());
    for (auto i = 0; i < batch->num_rows(); i++) {
        transactions->emplace_back(std::make_unique<Transaction>(0));
    }

    for (uint64_t i = 0; i < transactions->size(); i++) {
        auto &transaction = *(*transactions)[i];
        transaction.id_ = get_uint64(*id_column->GetScalar(i));
        transaction.start_time_ = get_uint64(*start_time->GetScalar(i));
        transaction.end_time_ = get_uint64(*end_time->GetScalar(i));

        // TODO, refactor this once it's working
        auto event_scalar_r = events->GetScalar(i);
        auto event_scalar = std::reinterpret_pointer_cast<arrow::ListScalar>(*event_scalar_r);
        auto size = event_scalar->value->length();
        std::vector<uint64_t> ids(size);
        for (uint64_t j = 0; j < ids.size(); j++) {
            auto v = get_uint64(*event_scalar->value->GetScalar(j));
            ids[j] = v;
        }
        transaction.events_ids_ = std::move(ids);
    }
    return std::move(transactions);
}

}  // namespace hermes