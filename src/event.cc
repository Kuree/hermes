#include "event.hh"

#include <variant>

#include "arrow.hh"
#include "arrow/api.h"
#include "arrow/ipc/reader.h"
#include "fmt/format.h"
#include "parquet/stream_writer.h"

namespace hermes {

uint64_t Event::event_id_count_ = 0;

Event::Event(uint64_t time) noexcept : time_(time), id_(event_id_count_++) {
    add_value(TIME_NAME, time);
    add_value(ID_NAME, id_);
}

bool Event::remove_value(const std::string &name) {
    if (name == TIME_NAME || name == ID_NAME) return false;
    if (values_.find(name) == values_.end()) return false;
    values_.erase(name);
    return true;
}

void Event::set_time(uint64_t time) {
    time_ = time;
    values_[TIME_NAME] = time;
}

void Event::set_id(uint64_t id) {
    id_ = id;
    values_[ID_NAME] = id;
}

template <class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

std::shared_ptr<arrow::Schema> get_schema(Event *event) {
    auto const &values = event->values();
    std::vector<std::shared_ptr<arrow::Field>> schema_vector;

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
        schema_vector.emplace_back(field);
    }

    return std::make_shared<arrow::Schema>(schema_vector);
}

std::pair<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::Schema>>
EventBatch::serialize() const noexcept {
    auto const error_return = std::make_pair(nullptr, nullptr);
    if (empty()) return error_return;
    // we assume it is already validated
    std::shared_ptr<arrow::Schema> schema;
    {
        auto *event = front().get();
        schema = get_schema(event);
    }
    // we need to initialize the type builder
    auto *pool = arrow::default_memory_pool();
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
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
    for (auto const &event : *this) {
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
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(builders.size());
    for (auto &builder : builders) {
        std::shared_ptr<arrow::Array> array;
        (void)builder->Finish(&array);
        arrays.emplace_back(array);
    }

    auto batch = arrow::RecordBatch::Make(schema, size(), arrays);
    return {batch, schema};
}

std::unique_ptr<EventBatch> EventBatch::deserialize(
    const std::shared_ptr<arrow::Table> &table) {  // NOLINT
    // construct the event batch
    auto event_batch = std::make_unique<EventBatch>();
    uint64_t num_rows = table->num_rows();

    event_batch->reserve(num_rows);
    // create each batch
    for (auto i = 0; i < num_rows; i++) {
        event_batch->emplace_back(std::make_shared<Event>(0));
    }

    auto bool_ = arrow::boolean();
    auto uint8 = arrow::uint8();
    auto uint16 = arrow::uint16();
    auto uint32 = arrow::uint32();
    auto uint64 = arrow::uint64();
    auto str_ = arrow::utf8();

    // look through each column and fill in data
    auto const &schema = table->schema();
    auto const &fields = schema->fields();
    for (int i = 0; i < table->num_columns(); i++) {
        auto const &name = fields[i]->name();
        auto type = fields[i]->type();
        auto const &column_chunks = table->column(i);
        for (auto chunk_idx = 0; chunk_idx < column_chunks->num_chunks(); chunk_idx++) {
            auto const &column = column_chunks->chunk(chunk_idx);
            for (int j = 0; j < column->length(); j++) {
                auto r_ = column->GetScalar(j);
                if (!r_.ok()) return nullptr;
                auto const &v = *r_;
                if (type->Equals(str_)) {
                    (*event_batch)[j]->add_value(name, get_string(v));
                } else if (type->Equals(uint8)) {
                    (*event_batch)[j]->add_value(name, get_uint8(v));
                } else if (type->Equals(uint16)) {
                    auto int_val_ = std::reinterpret_pointer_cast<arrow::UInt16Scalar>(v);
                    (*event_batch)[j]->add_value(name, get_uint16(v));
                } else if (type->Equals(uint32)) {
                    auto int_val_ = std::reinterpret_pointer_cast<arrow::UInt32Scalar>(v);
                    (*event_batch)[j]->add_value(name, get_uint32(v));
                } else if (type->Equals(uint64)) {
                    auto int_val = get_uint64(v);
                    if (name == Event::TIME_NAME) {
                        (*event_batch)[j]->set_time(int_val);
                    } else if (name == Event::ID_NAME) {
                        (*event_batch)[j]->set_id(int_val);
                    } else {
                        (*event_batch)[j]->add_value(name, int_val);
                    }
                } else if (type->Equals(bool_)) {
                    auto bool_val_ = std::reinterpret_pointer_cast<arrow::BooleanScalar>(v);
                    (*event_batch)[j]->add_value(name, get_bool(v));
                } else {
                    auto error_msg =
                        fmt::format("Unknown type {0} for column {1}", type->ToString(), name);
                    throw std::runtime_error(error_msg);
                }
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

void EventBatch::sort() {
    // we sort it based on time
    std::sort(begin(), end(), [](const std::shared_ptr<Event> &a, const std::shared_ptr<Event> &b) {
        return a->time() < b->time();
    });
}

Event *EventBatch::get_event(uint64_t id) {
    if (id_index_.empty()) {
        build_id_index();
    }
    if (id_index_.find(id) != id_index_.end()) {
        return id_index_.at(id);
    } else {
        return nullptr;
    }
}

EventBatch::iterator EventBatch::lower_bound(uint64_t time) {
    if (lower_bound_index_.empty()) {
        build_time_index();
    }
    auto it = lower_bound_index_.lower_bound(time);
    if (it == lower_bound_index_.end()) {
        return end();
    } else {
        return it->second;
    }
}

EventBatch::iterator EventBatch::upper_bound(uint64_t time) {
    if (upper_bounder_index_.empty()) {
        build_time_index();
    }

    auto it = upper_bounder_index_.upper_bound(time);
    if (it == upper_bounder_index_.end()) {
        return end();
    } else {
        return it->second;
    }
}

bool EventBatch::contains(uint64_t id) {
    if (id_index_.empty()) {
        build_id_index();
    }
    return id_index_.find(id) != id_index_.end();
}

void EventBatch::build_id_index() {
    for (auto const &e : *this) {
        id_index_.emplace(e->id(), e.get());
    }
}

void EventBatch::build_time_index() {
    for (auto it = this->begin(); it != this->end(); it++) {
        lower_bound_index_.try_emplace((*it)->time(), it);
        upper_bounder_index_[(*it)->time()] = it;
    }
}

}  // namespace hermes