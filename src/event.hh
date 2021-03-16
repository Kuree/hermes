#ifndef HERMES_EVENT_HH
#define HERMES_EVENT_HH

#include <map>
#include <string>
#include <variant>

namespace hermes {

class Event {
public:
    explicit Event(uint64_t time) noexcept : time_(time) {}
    template <typename T>
    bool add_value(const std::string &name, const T &value) noexcept {
        if (values_.find(name) != values_.end()) return false;
        values_.emplace(name, value);
        return true;
    }

    template <typename T>
    std::optional<T> get_value(const std::string &name) noexcept {
        if (values_.find(name) == values_.end()) {
            return std::nullopt;
        } else {
            auto const &entry = values_.at(name);
            return std::get<T>(entry);
        }
    }

    [[nodiscard]] uint64_t time() const { return time_; }

private:
    using EventValue = std::variant<uint64_t, uint32_t, uint16_t, uint8_t, std::string>;
    uint64_t time_;
    std::map<std::string, EventValue> values_;
};

}  // namespace hermes

#endif  // HERMES_EVENT_HH
