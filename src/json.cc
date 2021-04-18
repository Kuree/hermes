#include "json.hh"

namespace hermes::json {
rapidjson::Value serialize(rapidjson::MemoryPoolAllocator<> &allocator,
                           const std::shared_ptr<Event> &event) {
    using namespace rapidjson;
    Value result(kObjectType);
    auto const &values = event->values();

    for (auto const &[name, v] : values) {
        auto const *name_ptr = name.c_str();
        std::visit(overloaded{[&result, &allocator, name_ptr](uint8_t v) {
                                  set_member(result, allocator, name_ptr, v);
                              },
                              [&result, &allocator, name_ptr](uint16_t v) {
                                  set_member(result, allocator, name_ptr, v);
                              },
                              [&result, &allocator, name_ptr](uint32_t v) {
                                  set_member(result, allocator, name_ptr, v);
                              },
                              [&result, &allocator, name_ptr](uint64_t v) {
                                  set_member(result, allocator, name_ptr, v);
                              },
                              [&result, &allocator, name_ptr](bool v) {
                                  set_member(result, allocator, name_ptr, v);
                              },
                              [&result, &allocator, name_ptr](const std::string &v) {
                                  set_member(result, allocator, name_ptr, v);
                              }},
                   v);
    }

    return result;
}

}  // namespace hermes::json
