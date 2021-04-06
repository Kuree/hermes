#include "transaction.hh"

#include "arrow.hh"
#include "arrow/api.h"
#include "arrow/ipc/reader.h"
#include "parquet/stream_writer.h"

namespace hermes {

uint64_t Transaction::id_allocator_ = 0;

Transaction::Transaction() noexcept : id_(id_allocator_++) {}

bool Transaction::add_event(const Event *event) {
    if (finished_) return false;
    if (start_time_ > event->time()) {
        start_time_ = event->time();
    }
    if (end_time_ < event->time()) {
        end_time_ = event->time();
    }

    events_ids_.emplace_back(event->id());
    return true;
}

std::pair<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::Schema>>
TransactionBatch::serialize() const noexcept {
    auto error_return = std::make_pair(nullptr, nullptr);
    // we need to serialize a list of event ids
    auto const &list = *this;
    auto *pool = arrow::default_memory_pool();
    arrow::UInt64Builder id_builder(pool);
    arrow::UInt64Builder start_builder(pool);
    arrow::UInt64Builder end_builder(pool);
    arrow::BooleanBuilder finished_builder(pool);
    arrow::ListBuilder event_ids_list_builder(pool, std::make_shared<arrow::UInt64Builder>(pool));
    auto &event_ids_builder =
        *(reinterpret_cast<arrow::UInt64Builder *>(event_ids_list_builder.value_builder()));

    for (auto const &transaction : list) {
        (void)id_builder.Append(transaction->id());
        (void)start_builder.Append(transaction->start_time());
        (void)end_builder.Append(transaction->end_time());
        (void)finished_builder.Append(transaction->finished());
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
    if (!r.ok()) return error_return;
    std::shared_ptr<arrow::Array> start_array;
    r = start_builder.Finish(&start_array);
    if (!r.ok()) return error_return;
    std::shared_ptr<arrow::Array> end_array;
    r = end_builder.Finish(&end_array);
    if (!r.ok()) return error_return;
    std::shared_ptr<arrow::Array> finished_array;
    r = finished_builder.Finish(&finished_array);
    if (!r.ok()) return error_return;
    std::shared_ptr<arrow::Array> event_id_array;
    r = event_ids_list_builder.Finish(&event_id_array);
    if (!r.ok()) return error_return;

    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
        arrow::field("id", arrow::uint64()), arrow::field("start_time", arrow::uint64()),
        arrow::field("end_time", arrow::uint64()), arrow::field("finished", arrow::boolean()),
        arrow::field("events", arrow::list(arrow::uint64()))};
    auto schema = std::make_shared<arrow::Schema>(schema_vector);

    auto batch = arrow::RecordBatch::Make(
        schema, size(), {id_array, start_array, end_array, finished_array, event_id_array});
    return {batch, schema};
}

std::unique_ptr<TransactionBatch> TransactionBatch::deserialize(
    const std::shared_ptr<arrow::Table> &table) {
    uint64_t num_rows = table->num_rows();
    auto transactions = std::make_unique<TransactionBatch>();
    transactions->reserve(num_rows);

    auto id = table->column(0);
    auto start = table->column(1);
    auto end = table->column(2);
    auto finished = table->column(3);
    auto e = table->column(4);

    // need to iterate over the chunks
    for (auto idx = 0; idx < id->num_chunks(); idx++) {
        auto id_column = id->chunk(idx);
        auto start_time = start->chunk(idx);
        auto end_time = end->chunk(idx);
        auto finished_ = finished->chunk(idx);
        auto events = e->chunk(idx);

        for (auto i = 0; i < id_column->length(); i++) {
            auto tid = get_uint64(*id_column->GetScalar(i));
            auto transaction = std::make_unique<Transaction>(tid);

            transaction->start_time_ = get_uint64(*start_time->GetScalar(i));
            transaction->end_time_ = get_uint64(*end_time->GetScalar(i));
            transaction->finished_ = get_bool(*finished_->GetScalar(i));

            // TODO, refactor this once it's working
            auto event_scalar_r = events->GetScalar(i);
            auto event_scalar = std::reinterpret_pointer_cast<arrow::ListScalar>(*event_scalar_r);
            auto size = event_scalar->value->length();
            std::vector<uint64_t> ids(size);
            for (uint64_t j = 0; j < ids.size(); j++) {
                auto v = get_uint64(*event_scalar->value->GetScalar(j));
                ids[j] = v;
            }
            transaction->events_ids_ = std::move(ids);

            transactions->emplace_back(std::move(transaction));
        }
    }

    return std::move(transactions);
}

}  // namespace hermes