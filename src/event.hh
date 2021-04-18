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
    static constexpr auto NAME_NAME = "name";

    explicit Event(uint64_t time) noexcept;
    Event(const std::string &name, uint64_t time) noexcept;

    template <typename T>
    void add_value(const std::string &name, const T &value) noexcept {
        values_[name] = value;
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

    [[nodiscard]] uint64_t time() const { return *get_value<uint64_t>(TIME_NAME); }
    void set_time(uint64_t time);
    [[nodiscard]] uint64_t id() const { return *get_value<uint64_t>(ID_NAME); }
    void set_id(uint64_t id);
    [[nodiscard]] std::string name() const { return *get_value<std::string>(NAME_NAME); }
    void set_name(const std::string &name) { add_value(NAME_NAME, name); }

    [[nodiscard]] auto const &values() const { return values_; }

    using EventValue = std::variant<uint64_t, uint32_t, uint16_t, uint8_t, bool, std::string>;

    void static reset_id() { event_id_count_ = 0; }

private:
    std::map<std::string, EventValue> values_;

    static uint64_t event_id_count_;
};

// template class to visit event values
template <class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

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

    virtual bool contains(uint64_t id) = 0;

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

    void set_name(const std::string &name) {
        name_ = name;
        for (auto &elem : *this) {
            elem->set_name(name);
        }
    }
    [[nodiscard]] const std::string &name() const { return name_; }

private:
    std::vector<std::shared_ptr<T>> array_;
    std::string name_;
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

    bool contains(uint64_t id) override;

private:
    std::unordered_map<uint64_t, Event *> id_index_;
    std::map<uint64_t, EventBatch::iterator> lower_bound_index_;
    std::map<uint64_t, EventBatch::iterator> upper_bounder_index_;

    void build_id_index();
    void build_time_index();
};

// helper functions
class MessageBus;
bool parse_event_log_fmt(const std::string &filename, const std::string &event_name,
                         const std::string &fmt, const std::vector<std::string> &fields);
bool parse_event_log_fmt(const std::string &filename, const std::string &event_name,
                         const std::string &fmt, const std::vector<std::string> &fields,
                         MessageBus *bus);

}  // namespace hermes

#endif  // HERMES_EVENT_HH
