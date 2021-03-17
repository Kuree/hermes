#include "event.hh"
#include "plasma/client.h"
#include "plasma/common.h"

#include <type_traits>
#include <variant>

namespace hermes {

// base case
uint64_t get_variant_size(const Event::EventValue &) { return 0; }

template <typename T, typename... Ts>
uint64_t get_variant_size(const Event::EventValue &value) {
    if (std::holds_alternative<T>(value)) {
        if (std::is_same<std::string, T>::value) {
            auto const &v = std::get<T>(value);
            // null terminated string
            return v.size() + 1;
        } else {
            return sizeof(T);
        }
    } else {
        // recursive
        get_variant_size<Ts...>(value);
    }
}

uint64_t Event::size() const {
    uint64_t result = 0;
    for (auto const &[name, value] : values_) {
        result += get_variant_size(value);
    }
    return result;
}

void Event::serialize(const std::shared_ptr<arrow::Buffer> &buffer) const {
    
}

}  // namespace hermes