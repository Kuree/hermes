#ifndef HERMES_JSON_HH
#define HERMES_JSON_HH

#include "loader.hh"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "transaction.hh"

namespace hermes::json {
template <typename T, typename K, typename A>
void set_member(K &json_value, A &allocator, const char *name, const T &value) {
    rapidjson::Value key(name, allocator);  // NOLINT
    if constexpr (std::is_same<T, std::string>::value) {
        rapidjson::Value v(value.c_str(), allocator);
        json_value.AddMember(key, v, allocator);
    } else if constexpr (std::is_integral<T>::value) {
        json_value.AddMember(key.Move(), value, allocator);
    } else if constexpr (std::is_same<T, rapidjson::Value>::value) {
        rapidjson::Value v_copy(value, allocator);
        json_value.AddMember(key.Move(), v_copy.Move(), allocator);
    } else if constexpr (std::is_same<T, std::map<std::string, std::string>>::value) {
        rapidjson::Value v(rapidjson::kObjectType);
        for (auto const &[name, value_] : value) {
            set_member(v, allocator, name.c_str(), value_);
        }
        json_value.AddMember(key.Move(), v.Move(), allocator);
    } else {
        throw std::runtime_error("Unable to determine type for " + std::string(name));
    }
}

template <typename K, typename A>
void set_member(K &json_value, A &allocator, const char *name, const char *value) {
    rapidjson::Value key(name, allocator);  // NOLINT
    rapidjson::Value v(value, allocator);
    json_value.AddMember(key, v, allocator);
}

template <typename T>
void set_member(rapidjson::Document &document, const char *name, const T &value) {
    auto &allocator = document.GetAllocator();
    set_member(document, allocator, name, value);
}

static bool check_member(rapidjson::Document &document, const char *member_name) {
    return document.HasMember(member_name);
}

template <typename T>
static std::optional<T> get_member(rapidjson::Document &document, const char *member_name) {
    if (!check_member(document, member_name)) return std::nullopt;
    if constexpr (std::is_same<T, std::string>::value) {
        if (document[member_name].IsString()) {
            return std::string(document[member_name].GetString());
        }
    } else if constexpr (std::is_integral<T>::value && !std::is_same<T, bool>::value) {
        if (document[member_name].IsNumber()) {
            return document[member_name].template Get<T>();
        }
    } else if constexpr (std::is_same<T, bool>::value) {
        if (document[member_name].IsBool()) {
            return document[member_name].GetBool();
        }
    }
    return std::nullopt;
}

rapidjson::Value serialize(rapidjson::MemoryPoolAllocator<> &allocator,
                           const std::shared_ptr<Event> &event);
rapidjson::Value serialize(rapidjson::MemoryPoolAllocator<> &allocator,
                           const std::shared_ptr<EventBatch> &batch);
rapidjson::Value serialize(rapidjson::MemoryPoolAllocator<> &allocator,
                           const TransactionData &data);
rapidjson::Value serialize(rapidjson::MemoryPoolAllocator<> &allocator,
                           const std::shared_ptr<TransactionStream> &stream);
std::string serialize(const rapidjson::Document &document, bool pretty_print);
std::string serialize(const rapidjson::Value &value, bool pretty_print);

}  // namespace hermes::json

#endif  // HERMES_JSON_HH
