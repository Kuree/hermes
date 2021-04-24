#include "event.hh"

#include <fstream>
#include <regex>
#include <variant>

#include "arrow.hh"
#include "arrow/api.h"
#include "arrow/ipc/reader.h"
#include "fmt/format.h"
#include "parquet/stream_writer.h"
#include "pubsub.hh"

namespace hermes {

std::atomic<uint64_t> Event::event_id_count_ = 0;

Event::Event(uint64_t time) noexcept : Event("", time) {}

Event::Event(const std::string &name, uint64_t time) noexcept {
    add_value(TIME_NAME, time);
    add_value(ID_NAME, event_id_count_++);
    add_value(NAME_NAME, name);
}

bool Event::remove_value(const std::string &name) {
    if (name == TIME_NAME || name == ID_NAME) return false;
    if (values_.find(name) == values_.end()) return false;
    values_.erase(name);
    return true;
}

void Event::set_time(uint64_t time) { values_[TIME_NAME] = time; }

void Event::set_id(uint64_t id) { values_[ID_NAME] = id; }

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
                    (*event_batch)[j]->add_value(name, int_val);
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

enum class ValueType { Int, Hex, Str, Time };
// we need a ways to parse printf format into regex
auto parse_fmt(const std::string &format, std::vector<ValueType> &types) {
    // we are interested in any $display related formatting
    // hand-rolled FSM-based parser
    int state = 0;
    std::string regex_data;
    regex_data.reserve(format.size() * 2);
    ;

    for (auto c : format) {
        if (state == 0) {
            if (c == '\\') {
                // escape mode
                state = 2;
            } else if (c == '%') {
                state = 1;
            } else {
                regex_data.append(std::string(1, c));
            }
        } else if (state == 1) {
            if (isdigit(c)) {
                continue;
            } else if (c == 'd') {
                // put number regex here
                types.emplace_back(ValueType::Int);
                regex_data.append(R"(\s?(\d+))");
            } else if (c == 't') {
                types.emplace_back(ValueType::Time);
                regex_data.append(R"(\s?(\d+))");
            } else if (c == 'x' || c == 'X') {
                types.emplace_back(ValueType::Hex);
                regex_data.append(R"(\s?([\da-fA-F]+))");
            } else if (c == 's') {
                types.emplace_back(ValueType::Str);
                regex_data.append(R"((\w+))");
            } else if (c == 'm') {
                types.emplace_back(ValueType::Str);
                regex_data.append(R"(([\w$_\d.]+))");
            } else {
                throw std::runtime_error("Unknown formatter " + std::string(1, c));
            }
            state = 0;
        } else {
            if (c == '%') {
                // no need to escape this at all
                regex_data.append(std::string(1, c));
            } else {
                regex_data.append(std::string(1, '\\'));
                regex_data.append(std::string(1, c));
            }
            state = 0;
        }
    }
    // set the regex
    return std::regex(regex_data);
}

bool parse_event_log_fmt(const std::string &filename, const std::string &event_name,
                         const std::string &fmt, const std::vector<std::string> &fields) {
    return parse_event_log_fmt(filename, event_name, fmt, fields, MessageBus::default_bus());
}

bool parse_event_log_fmt(const std::string &filename, const std::string &event_name,
                         const std::string &fmt, const std::vector<std::string> &fields,
                         MessageBus *bus) {
    // need to open the file and parse the format
    std::vector<ValueType> types;
    auto re = parse_fmt(fmt, types);
    if (types.size() != fields.size()) return false;
    // need to open the file and read out, assume this is a huge files, we need to
    // read out as a stream
    std::ifstream stream(filename);
    if (stream.bad()) return false;
    std::string line;

    while (std::getline(stream, line)) {
        if (line.empty()) continue;
        // parse it
        std::smatch matches;
        if (std::regex_search(line, matches, re)) {
            // new event
            auto event = std::make_shared<Event>(0);
            event->set_name(event_name);
            // convert types and log values
            for (auto i = 1u; i < matches.size(); i++) {
                // need to convert the types
                auto idx = i - 1;
                auto type = types[idx];
                auto const &match = matches[i];
                switch (type) {
                    case ValueType::Int: {
                        auto value = std::stol(match.str());
                        event->add_value<uint32_t>(fields[idx], value);
                        break;
                    }
                    case ValueType::Time: {
                        auto value = std::stol(match.str());
                        event->add_value<uint64_t>(fields[idx], value);
                        break;
                    }
                    case ValueType::Hex: {
                        auto value = std::stol(match.str(), nullptr, 16);
                        event->add_value<uint32_t>(fields[idx], value);
                        break;
                    }
                    case ValueType::Str: {
                        event->add_value<std::string>(fields[idx], match.str());
                        break;
                    }
                }
            }
            bus->publish(event_name, event);
        }
    }
    stream.close();

    return true;
}

}  // namespace hermes