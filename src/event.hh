#ifndef HERMES_EVENT_HH
#define HERMES_EVENT_HH

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <variant>
#include <vector>

namespace arrow {
class Buffer;
}  // namespace arrow

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

    template <typename T>
    bool change_value(const std::string &name, const T &value) noexcept {
        if (values_.find(name) == values_.end()) return false;
        values_[name] = value;
        return true;
    }

    [[nodiscard]] uint64_t time() const { return time_; }
    void set_time(uint64_t time) { time_ = time; }

    [[nodiscard]] auto const &values() const { return values_; }

    using EventValue = std::variant<uint64_t, uint32_t, uint16_t, uint8_t, std::string>;

private:
    uint64_t time_;
    std::map<std::string, EventValue> values_;
};

// a batch of events
class EventBatch : public std::vector<std::unique_ptr<Event>> {
public:
    std::shared_ptr<arrow::Buffer> serialize(
        const std::function<std::shared_ptr<arrow::Buffer>(uint64_t)> &buffer_allocator);
    [[nodiscard]] bool validate() const noexcept;

    // factory method to construct event batch
    static std::unique_ptr<EventBatch> deserialize(const std::shared_ptr<arrow::Buffer> &buffer);
};

}  // namespace hermes

#endif  // HERMES_EVENT_HH
