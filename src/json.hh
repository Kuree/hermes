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
        throw std::runtime_error("Unable type for " + std::string(name));
    }
}

template <typename T>
void set_member(rapidjson::Document &document, const char *name, const T &value) {
    auto &allocator = document.GetAllocator();
    set_member(document, allocator, name, value);
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
