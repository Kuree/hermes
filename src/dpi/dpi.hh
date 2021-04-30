#ifndef HERMES_DPI_HH
#define HERMES_DPI_HH

#include <atomic>
#include <map>
#include <mutex>
#include <thread>

#include "logger.hh"
#include "serializer.hh"
#include "svdpi.h"
#include "tracker.hh"

// all DPI uses default message bus
constexpr auto max_events_size = 1 << 16;

class SpinLock {
    std::atomic_flag locked = ATOMIC_FLAG_INIT;

public:
    void lock() {
        while (locked.test_and_set(std::memory_order_acquire))
            ;
    }
    void unlock() { locked.clear(std::memory_order_release); }
};

class DPILogger : public hermes::Logger {
public:
    explicit DPILogger(const std::string &name) : hermes::Logger(name) {}

    template <typename T>
    inline void set_value(const std::string &name, const T &value, uint64_t idx) {
        if (events_.size() > idx && idx < max_events_size) {
            {
                event_locks_[idx].lock();
                events_[idx]->template add_value(name, value);
                event_locks_[idx].unlock();
            }
        }
    }
    [[nodiscard]] uint64_t num_events() const { return events_.size(); }

    inline void set_time(uint64_t index, uint64_t time) { events_[index]->set_time(time); }

    void create_events(uint64_t num_events);
    void send_events();

    [[nodiscard]] const std::vector<std::shared_ptr<hermes::Event>> &events() const {
        return events_;
    }

    void add_thread(const std::function<void()> &func);
    void join();

    ~DPILogger();

private:
    // batch
    std::mutex events_lock_;
    std::vector<std::shared_ptr<hermes::Event>> events_;
    // atomic variables implemented as a spin lock to protect events
    std::array<SpinLock, max_events_size> event_locks_;

    std::vector<std::thread> threads_;
};

class DPITracker : public hermes::Tracker {
public:
    // we don't want this DPI tracker to actually receive any messages
    static constexpr auto *topic = "__RESERVED__";
    explicit DPITracker(const std::string &name) : hermes::Tracker(topic) {
        set_transaction_name(name);
    }

    void track(hermes::Event *event) override {}
};

// DPI part
extern "C" {
[[maybe_unused]] void hermes_set_output_dir(const char *directory);
[[maybe_unused]] void *hermes_create_logger(const char *name);
[[maybe_unused]] void hermes_create_events(void *logger, svOpenArrayHandle times);
[[maybe_unused]] void hermes_create_events_id(void *logger, svOpenArrayHandle times,
                                              svOpenArrayHandle event_ids);
[[maybe_unused]] void hermes_set_values_uint8(void *logger, svOpenArrayHandle names,
                                              svOpenArrayHandle array);
[[maybe_unused]] void hermes_set_values_uint16(void *logger, svOpenArrayHandle names,
                                               svOpenArrayHandle array);
[[maybe_unused]] void hermes_set_values_uint32(void *logger, svOpenArrayHandle names,
                                               svOpenArrayHandle array);
[[maybe_unused]] void hermes_set_values_uint64(void *logger, svOpenArrayHandle names,
                                               svOpenArrayHandle array);
[[maybe_unused]] void hermes_set_values_bool(void *logger, svOpenArrayHandle names,
                                             svOpenArrayHandle array);
[[maybe_unused]] void hermes_set_values_string(void *logger, svOpenArrayHandle names,
                                               svOpenArrayHandle array);
[[maybe_unused]] void hermes_send_events(void *logger);

// tracker
[[maybe_unused]] void *hermes_create_tracker(const char *name);
[[maybe_unused]] void *hermes_tracker_new_transaction(void *tracker);
[[maybe_unused]] void hermes_transaction_finish(void *transaction);
[[maybe_unused]] void hermes_retire_transaction(void *tracker, void *transaction);
[[maybe_unused]] void hermes_add_event_transaction(void *transaction, void *event);

// clean up code
[[maybe_unused]] void hermes_final();

// helper functions
[[maybe_unused]] void hermes_add_dummy_serializer(const char *topic);
[[maybe_unused]] void hermes_set_serializer_dir(const char *topic);
}

#endif  // HERMES_DPI_HH
