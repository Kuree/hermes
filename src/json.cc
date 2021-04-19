#include "json.hh"

#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace hermes::json {
rapidjson::Value serialize(rapidjson::MemoryPoolAllocator<> &allocator,
                           const std::shared_ptr<Event> &event) {
    using namespace rapidjson;
    Value result(kObjectType);
    auto const &values = event->values();
    set_member(result, allocator, "type", "event");
    Value event_data(kObjectType);

    for (auto const &[name, v] : values) {
        auto const *name_ptr = name.c_str();
        std::visit(overloaded{[&event_data, &allocator, name_ptr](uint8_t v) {
                                  set_member(event_data, allocator, name_ptr, v);
                              },
                              [&event_data, &allocator, name_ptr](uint16_t v) {
                                  set_member(event_data, allocator, name_ptr, v);
                              },
                              [&event_data, &allocator, name_ptr](uint32_t v) {
                                  set_member(event_data, allocator, name_ptr, v);
                              },
                              [&event_data, &allocator, name_ptr](uint64_t v) {
                                  set_member(event_data, allocator, name_ptr, v);
                              },
                              [&event_data, &allocator, name_ptr](bool v) {
                                  set_member(event_data, allocator, name_ptr, v);
                              },
                              [&event_data, &allocator, name_ptr](const std::string &v) {
                                  set_member(event_data, allocator, name_ptr, v);
                              }},
                   v);
    }
    set_member(result, allocator, "value", event_data);

    return result;
}

template <typename T>
rapidjson::Value serialize_batch(rapidjson::MemoryPoolAllocator<> &allocator,
                           const std::shared_ptr<T> &batch) {
    using namespace rapidjson;
    rapidjson::Value result(kArrayType);
    for (auto const &entry: *batch) {
        result.PushBack(serialize(allocator, entry), allocator);
    }
    return result;
}

rapidjson::Value serialize(rapidjson::MemoryPoolAllocator<> &allocator,
                           const std::shared_ptr<EventBatch> &batch) {
    return serialize_batch(allocator, batch);
}

rapidjson::Value serialize(rapidjson::MemoryPoolAllocator<> &allocator,
                           const TransactionData &data) {
    using namespace rapidjson;

    // notice that transaction data is a recursive structure
    if (data.is_group()) {
        Value result(kArrayType);
        auto group_data = data.group->values;
        for (auto const &d : group_data) {
            auto v = serialize(allocator, d);
            result.PushBack(std::move(v), allocator);
        }
        return result;
    } else {
        // this is a transaction data
        Value t(kObjectType);
        set_member(t, allocator, "type", "transaction");
        set_member(t, allocator, "name", data.transaction->name());
        // array
        Value array(kArrayType);
        for (auto const &event : *data.events) {
            if (!event) {
                // push a null value. this actually indicates an internal error
                // since we actually expect we've saved everything
                array.PushBack(Value(), allocator);
                continue;
            }
            auto e = serialize(allocator, event);
            array.PushBack(std::move(e), allocator);
        }
        set_member(t, allocator, "events", array);
        return t;
    }
}

rapidjson::Value serialize(rapidjson::MemoryPoolAllocator<> &allocator,
                           const std::shared_ptr<TransactionStream> &stream) {
    using namespace rapidjson;
    Value result(kArrayType);

    for (auto const &data : *stream) {
        auto v = serialize(allocator, data);
        result.PushBack(std::move(v), allocator);
    }
    return result;
}

std::string serialize(const rapidjson::Document &document, bool pretty_print) {
    using namespace rapidjson;
    StringBuffer buffer;
    if (pretty_print) {
        PrettyWriter w(buffer);
        document.Accept(w);
    } else {
        Writer w(buffer);
        document.Accept(w);
    }
    const auto *s = buffer.GetString();
    return s;
}

std::string serialize(const rapidjson::Value &value, bool pretty_print) {
    using namespace rapidjson;
    StringBuffer buffer;
    if (pretty_print) {
        PrettyWriter w(buffer);
        value.Accept(w);
    } else {
        Writer w(buffer);
        value.Accept(w);
    }
    const auto *s = buffer.GetString();
    return s;
}

}  // namespace hermes::json
