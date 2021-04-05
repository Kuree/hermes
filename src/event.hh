#ifndef HERMES_EVENT_HH
#define HERMES_EVENT_HH

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace arrow {
class RecordBatch;
class Schema;
class Table;
}  // namespace arrow

namespace hermes {

class Event : public std::enable_shared_from_this<Event> {
public:
    static constexpr auto TIME_NAME = "time";
    static constexpr auto ID_NAME = "id";

    explicit Event(uint64_t time) noexcept;
    template <typename T>
    bool add_value(const std::string &name, const T &value) noexcept {
        if (values_.find(name) != values_.end()) return false;
        values_.emplace(name, value);
        return true;
    }

    template <typename T>
    std::optional<T> get_value(const std::string &name) const noexcept {
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

    bool remove_value(const std::string &name);
    [[nodiscard]] bool has_value(const std::string &name) const noexcept {
        return values_.find(name) != values_.end();
    }

    [[nodiscard]] uint64_t time() const { return time_; }
    void set_time(uint64_t time);
    [[nodiscard]] uint64_t id() const { return id_; }
    void set_id(uint64_t id);

    [[nodiscard]] auto const &values() const { return values_; }

    using EventValue = std::variant<uint64_t, uint32_t, uint16_t, uint8_t, bool, std::string>;

private:
    uint64_t time_;
    uint64_t id_;
    std::map<std::string, EventValue> values_;

    static uint64_t event_id_count_;
};

// a batch of events
class EventBatch : public std::vector<std::shared_ptr<Event>> {
public:
    [[nodiscard]] std::pair<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::Schema>>
    serialize() const noexcept;
    [[nodiscard]] bool validate() const noexcept;

    // factory method to construct event batch
    static std::unique_ptr<EventBatch> deserialize(const std::shared_ptr<arrow::Table> &table);
    static std::unique_ptr<EventBatch> deserialize(const std::shared_ptr<arrow::Table> &table,
                                                   const std::vector<uint64_t> &row_groups);
    static std::unique_ptr<EventBatch> deserialize(const std::shared_ptr<arrow::Table> &table,
                                                   uint64_t idx);

    Event *get_event(uint64_t id);

    void set_event_name(std::string name) { event_name_ = std::move(name); }
    [[nodiscard]] const std::string &event_name() const { return event_name_; }

private:
    std::unordered_map<uint64_t, Event *> index_;
    std::string event_name_;

    void build_index();
};

}  // namespace hermes

#endif  // HERMES_EVENT_HH
