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
        (void)event_ids_builder.AppendValues(ids.data(), static_cast<int64_t>(ids.size()));
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
        schema, static_cast<int64_t>(size()),
        {id_array, start_array, end_array, finished_array, event_id_array});
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

            transaction->events_ids_ = get_uint64s(*events->GetScalar(i));

            transactions->emplace_back(std::move(transaction));
        }
    }

    return std::move(transactions);
}

TransactionBatch::iterator TransactionBatch::lower_bound(uint64_t time) {
    if (time_lower_bound_.empty()) {
        build_time_index();
    }

    auto it = time_lower_bound_.lower_bound(time);
    if (it == time_lower_bound_.end()) {
        return end();
    } else {
        return it->second;
    }
}

void TransactionBatch::build_time_index() {
    // we assume the transaction is sorted by end time already
    for (auto it = begin(); it != end(); it++) {
        auto const &transaction = *it;
        time_lower_bound_.try_emplace(transaction->end_time_, it);
    }
}

void TransactionBatch::build_id_index() {
    if (!id_index_.empty()) return;

    for (auto const &t : *this) {
        id_index_.emplace(t->id(), t.get());
    }
}

bool TransactionBatch::contains(uint64_t id) {
    if (id_index_.empty()) {
        build_id_index();
    }
    return id_index_.find(id) != id_index_.end();
}

std::shared_ptr<Transaction> TransactionBatch::at(uint64_t id) const {
    if (id_index_.find(id) != id_index_.end()) {
        auto *ptr = id_index_.at(id);
        return ptr->shared_from_this();
    }
    return nullptr;
}

void TransactionBatch::sort() {
    std::stable_sort(begin(), end(),
                     [](const auto &a, const auto &b) { return a->end_time() < b->end_time(); });
}

uint64_t TransactionGroup::id_allocator_ = 0;

TransactionGroup::TransactionGroup(uint64_t id) : id_(id) {}

TransactionGroup::TransactionGroup() : TransactionGroup(id_allocator_++) {}

void TransactionGroup::add_transaction(const std::shared_ptr<TransactionGroup> &group) {
    transactions_.emplace_back(group->id());
    transaction_masks_.emplace_back(true);
    if (end_time_ < group->end_time_) {
        end_time_ = group->end_time_;
    }
    if (start_time_ > group->start_time_) {
        start_time_ = group->start_time_;
    }
}

void TransactionGroup::add_transaction(const std::shared_ptr<Transaction> &transaction) {
    transactions_.emplace_back(transaction->id());
    transaction_masks_.emplace_back(false);
    if (end_time_ < transaction->end_time()) {
        end_time_ = transaction->end_time();
    }
    if (start_time_ > transaction->start_time()) {
        start_time_ = transaction->start_time();
    }
}

std::pair<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::Schema>>
TransactionGroupBatch::serialize() const noexcept {
    auto error_return = std::make_pair(nullptr, nullptr);
    // we need to serialize a list of event ids
    auto const &list = *this;
    auto *pool = arrow::default_memory_pool();
    arrow::UInt64Builder id_builder(pool);
    arrow::UInt64Builder start_builder(pool);
    arrow::UInt64Builder end_builder(pool);
    // we use a bit mask to mask ids that are
    arrow::ListBuilder transaction_ids_list_builder(pool,
                                                    std::make_shared<arrow::UInt64Builder>(pool));
    arrow::ListBuilder transaction_mask_list_builder(pool,
                                                     std::make_shared<arrow::BooleanBuilder>(pool));
    auto &transaction_ids_builder =
        *(reinterpret_cast<arrow::UInt64Builder *>(transaction_ids_list_builder.value_builder()));
    auto &transaction_masks_builder =
        *(reinterpret_cast<arrow::BooleanBuilder *>(transaction_mask_list_builder.value_builder()));

    for (auto const &transaction : list) {
        (void)id_builder.Append(transaction->id());
        (void)start_builder.Append(transaction->start_time());
        (void)end_builder.Append(transaction->end_time());
        // indicate the start of a new list row
        (void)transaction_ids_list_builder.Append();
        (void)transaction_mask_list_builder.Append();
        (void)transaction_ids_builder.AppendValues(transaction->transactions());
        (void)transaction_masks_builder.AppendValues(transaction->transaction_masks());
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
    std::shared_ptr<arrow::Array> transaction_ids_array;
    r = transaction_ids_list_builder.Finish(&transaction_ids_array);
    if (!r.ok()) return error_return;
    std::shared_ptr<arrow::Array> transaction_masks_Array;
    r = transaction_mask_list_builder.Finish(&transaction_masks_Array);
    if (!r.ok()) return error_return;

    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
        arrow::field("id", arrow::uint64()), arrow::field("start_time", arrow::uint64()),
        arrow::field("end_time", arrow::uint64()),
        arrow::field("transaction_ids", arrow::list(arrow::uint64())),
        arrow::field("transaction_masks", arrow::list(arrow::boolean()))};
    auto schema = std::make_shared<arrow::Schema>(schema_vector);

    auto batch = arrow::RecordBatch::Make(
        schema, static_cast<int64_t>(size()),
        {id_array, start_array, end_array, transaction_ids_array, transaction_masks_Array});
    return {batch, schema};
}

std::unique_ptr<TransactionGroupBatch> TransactionGroupBatch::deserialize(
    const std::shared_ptr<arrow::Table> &table) {
    uint64_t num_rows = table->num_rows();
    auto transactions = std::make_unique<TransactionGroupBatch>();
    transactions->reserve(num_rows);

    auto id = table->column(0);
    auto start = table->column(1);
    auto end = table->column(2);
    auto ids = table->column(3);
    auto masks = table->column(4);

    // need to iterate over the chunks
    for (auto idx = 0; idx < id->num_chunks(); idx++) {
        auto id_column = id->chunk(idx);
        auto start_time = start->chunk(idx);
        auto end_time = end->chunk(idx);
        auto ids_ = ids->chunk(idx);
        auto masks_ = masks->chunk(idx);

        for (auto i = 0; i < id_column->length(); i++) {
            auto tid = get_uint64(*id_column->GetScalar(i));
            auto transaction = std::make_unique<TransactionGroup>(tid);

            transaction->start_time_ = get_uint64(*start_time->GetScalar(i));
            transaction->end_time_ = get_uint64(*end_time->GetScalar(i));

            transaction->transactions_ = get_uint64s(*ids_->GetScalar(i));
            transaction->transaction_masks_ = get_bools(*masks_->GetScalar(i));

            transactions->emplace_back(std::move(transaction));
        }
    }

    return std::move(transactions);
}

void TransactionGroupBatch::sort() {
    std::sort(begin(), end(),
              [](const auto &a, const auto &b) { return a->end_time_ < b->end_time_; });
}

bool TransactionGroupBatch::contains(uint64_t id) {
    if (id_index_.empty()) {
        build_index();
    }
    return id_index_.find(id) != id_index_.end();
}

std::shared_ptr<TransactionGroup> TransactionGroupBatch::at(uint64_t id) {
    if (id_index_.empty())
        return nullptr;
    else
        id_index_.at(id)->shared_from_this();
}

void TransactionGroupBatch::build_index() {
    for (auto const &t : *this) {
        id_index_.emplace(t->id(), t.get());
    }
}

}  // namespace hermes