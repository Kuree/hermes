#ifndef HERMES_TRANSACTION_HH
#define HERMES_TRANSACTION_HH

#include "event.hh"

namespace hermes {

class Transaction {
public:
    explicit Transaction(uint64_t id) : id_(id) {}
    void add_event(std::unique_ptr<Event> &event) { events_.emplace_back(std::move(event)); }
    void finish() { is_done_ = true; }
    [[nodiscard]] bool finished() const { return is_done_; }

private:
    uint64_t id_;
    uint64_t start_time_ = 0;
    uint64_t end_time_ = 0;
    bool is_done_ = false;
    std::vector<std::unique_ptr<Event>> events_;
};

}  // namespace hermes

#endif  // HERMES_TRANSACTION_HH
