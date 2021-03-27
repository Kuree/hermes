#ifndef HERMES_DPI_HH
#define HERMES_DPI_HH

#include <map>
#include <mutex>

#include "logger.hh"

// all DPI uses default message bus

class DPILogger : hermes::Logger {
public:
    explicit DPILogger(const std::string &name) : hermes::Logger(name) {}

    template <typename T>
    inline void set_value(const std::string &name, const T &value, uint64_t idx) {
        if (events_.size() > idx) {
            events_[idx]->template add_value(name, value);
        }
    }

    void create_events(uint64_t num_events);
    void send_events();

private:
    // batch
    std::mutex events_lock_;
    std::vector<std::shared_ptr<hermes::Event>> events_;
};

// DPI part
extern "C" {
void *hermes_create_logger(char *name);
}

#endif  // HERMES_DPI_HH
