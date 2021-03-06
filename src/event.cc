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
    std::vector<std::shared_ptr<arrow::Field>> schema_vector;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    hermes::serialize(this, schema_vector, arrays);
    auto schema = std::make_shared<arrow::Schema>(schema_vector);

    auto batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(size()), arrays);
    return {batch, schema};
}

std::unique_ptr<EventBatch> EventBatch::deserialize(const arrow::Table *table) {  // NOLINT
    // construct the event batch
    auto event_batch = std::make_unique<EventBatch>();
    uint64_t num_rows = table->num_rows();

    event_batch->reserve(num_rows);
    // create each batch
    for (auto i = 0u; i < num_rows; i++) {
        event_batch->emplace_back(std::make_shared<Event>(0));
    }

    auto field_names = table->schema()->field_names();
    // we need all the fields
    auto fields = std::unordered_set<std::string>(field_names.begin(), field_names.end());
    hermes::deserialize(event_batch.get(), table, fields);

    return event_batch;
}

bool EventBatch::validate() const noexcept {
    // make sure they have the same schema
    if (empty()) return true;
    auto const &ref = this->front()->values();
    for (uint64_t i = 1; i < size(); i++) {
        auto const &event = (*this)[i];
        auto const &values = event->values();
        if (!same_schema(ref, values)) return false;
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

bool same_schema(const std::map<std::string, AttributeValue> &ref,
                 const std::map<std::string, AttributeValue> &target) {
    if (target.size() != ref.size()) return false;
    return !std::any_of(ref.begin(), ref.end(), [&target](const auto &iter) {
        auto const &[name, v] = iter;
        if (target.find(name) == target.end()) {
            return true;
        }
        if (v.index() != target.at(name).index()) return true;
        return false;
    });
}

}  // namespace hermes