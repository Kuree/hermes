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

template <typename T, typename K>
class Batch {
public:
    virtual void sort() = 0;
    [[nodiscard]] virtual std::pair<std::shared_ptr<arrow::RecordBatch>,
                                    std::shared_ptr<arrow::Schema>>
    serialize() const noexcept = 0;

    virtual std::shared_ptr<K> where(const std::function<bool(const std::shared_ptr<T> &)> &func) {
        auto ptr = std::make_shared<K>();
        for (auto const &elem : *this) {
            if (func(elem)) {
                ptr->emplace_back(elem);
            }
        }
        return ptr;
    }

    // vector interface
    using iterator = typename std::vector<std::shared_ptr<T>>::iterator;
    [[nodiscard]] auto begin() const { return array_.begin(); }
    auto begin() { return array_.begin(); }
    [[nodiscard]] auto end() const { return array_.end(); }
    auto end() { return array_.end(); }
    auto emplace_back(std::shared_ptr<T> ptr) { array_.emplace_back(std::move(ptr)); }
    [[nodiscard]] auto size() const { return array_.size(); }
    [[nodiscard]] auto front() const { return array_.front(); }
    [[nodiscard]] auto empty() const { return array_.empty(); }
    void reserve(size_t size) { array_.reserve(size); }
    void resize(size_t size) { array_.resize(size); }
    template <typename Type>
    inline void resize(size_t size, const Type &value) {
        array_.resize(size, value);
    }
    void clear() { array_.clear(); }
    const std::shared_ptr<T> &operator[](uint64_t index) const { return array_[index]; }
    std::shared_ptr<T> &operator[](uint64_t index) { return array_[index]; }

    template <typename InputT>
    void insert(const iterator &pos, InputT first, InputT last) {
        array_.insert(pos, first, last);
    }

private:
    std::vector<std::shared_ptr<T>> array_;
};

// a batch of events
class EventBatch : public Batch<Event, EventBatch> {
public:
    [[nodiscard]] std::pair<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::Schema>>
    serialize() const noexcept override;
    [[nodiscard]] bool validate() const noexcept;
    void sort() override;

    // factory method to construct event batch
    static std::unique_ptr<EventBatch> deserialize(const std::shared_ptr<arrow::Table> &table);

    Event *get_event(uint64_t id);
    EventBatch::iterator lower_bound(uint64_t time);
    EventBatch::iterator upper_bound(uint64_t time);

    void set_event_name(std::string name) { event_name_ = std::move(name); }
    [[nodiscard]] const std::string &event_name() const { return event_name_; }

private:
    std::unordered_map<uint64_t, Event *> id_index_;
    std::map<uint64_t, EventBatch::iterator> lower_bound_index_;
    std::map<uint64_t, EventBatch::iterator> upper_bounder_index_;
    std::string event_name_;

    void build_id_index();
    void build_time_index();
};

}  // namespace hermes

#endif  // HERMES_EVENT_HH
