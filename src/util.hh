#ifndef HERMES_UTIL_HH
#define HERMES_UTIL_HH

#include <algorithm>
#include <string>
#include <vector>

namespace hermes {

// for easy enum string manipulation
template <typename T>
struct EnumString {
    static std::vector<std::string> data;
};

template <typename T>
struct EnumConstRefHolder {
    T val;
    explicit EnumConstRefHolder(T val) : val(val) {}
};

template <typename T>
std::string to_string(const EnumConstRefHolder<T> &val) {
    return EnumString<T>::data[static_cast<uint32_t>(val.val)];
}

template <typename T>
T from_str(const std::string &val) {
    auto it = std::find(EnumString<T>::data.begin(), EnumString<T>::data.end(), val);
    T e;
    if (it != EnumString<T>::data.end()) {
        e = static_cast<T>(std::distance(EnumString<T>::data.begin(), it));
    }
    return e;
}

std::string which(const std::string &name);

namespace os {
uint64_t get_total_system_memory();
}

namespace string {
std::vector<std::string> split(const std::string &str, const std::string &delimiter);
}  // namespace string

#define ENUM(name, ...)              \
    enum class name { __VA_ARGS__ }; \
    template <>                      \
    std::vector<std::string> EnumString<name>::data = string::split(#__VA_ARGS__, " ,")
}  // namespace hermes
#undef NUM_ARGS

#endif  // HERMES_UTIL_HH
