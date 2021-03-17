#ifndef HERMES_PLASMA_HH
#define HERMES_PLASMA_HH

#include <memory>
#include <string>

#include "event.hh"
#include "process.hh"
#include "util.hh"

namespace plasma {
class PlasmaClient;
}

namespace hermes {

class PlasmaServer {
public:
    PlasmaServer(uint64_t mem_size, std::string server_path) noexcept
        : mem_size_(mem_size), server_path_(std::move(server_path)) {}
    explicit PlasmaServer(uint64_t mem_size) noexcept : mem_size_(mem_size) {}

    bool start();
    void stop();

    void set_pipe_filename(const std::string &name) { pipe_filename_ = name; }

private:
    std::string server_path_;
    std::unique_ptr<Process> server_ = nullptr;
    uint64_t mem_size_;
    std::string pipe_filename_;
};

ENUM(EventType, Object, END);    // NOLINT

void send_event_batch(const EventBatch &batch, plasma::PlasmaClient *client);

}  // namespace hermes

#endif  // HERMES_PLASMA_HH
