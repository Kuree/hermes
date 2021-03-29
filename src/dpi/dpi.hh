#ifndef HERMES_DPI_HH
#define HERMES_DPI_HH

#include <map>
#include <mutex>

#include "logger.hh"
#include "svdpi.h"

// all DPI uses default message bus

class DPILogger : public hermes::Logger {
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
    ~DPILogger();

private:
    // batch
    std::mutex events_lock_;
    std::vector<std::shared_ptr<hermes::Event>> events_;
};

// DPI part
extern "C" {
[[maybe_unused]] void hermes_set_output_dir(const char *directory);
[[maybe_unused]] void *hermes_create_logger(const char *name);
[[maybe_unused]] void hermes_create_events(void *logger, const char *name, uint64_t num_events);
[[maybe_unused]] void hermes_set_values_uint8(void *logger, const char *name,
                                              svOpenArrayHandle array);
[[maybe_unused]] void hermes_set_values_uint16(void *logger, const char *name,
                                               svOpenArrayHandle array);
[[maybe_unused]] void hermes_set_values_uin32(void *logger, const char *name,
                                              svOpenArrayHandle array);
[[maybe_unused]] void hermes_set_values_uint64(void *logger, const char *name,
                                               svOpenArrayHandle array);
[[maybe_unused]] void hermes_set_values_string(void *logger, const char *name,
                                               svOpenArrayHandle array);
[[maybe_unused]] void hermes_send_events(void *logger);
[[maybe_unused]] void hermes_final();
}

#endif  // HERMES_DPI_HH
