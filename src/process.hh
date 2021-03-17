#ifndef HERMES_PROCESS_HH
#define HERMES_PROCESS_HH

#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// forward declare
namespace subprocess {
class Popen;
}

namespace hermes {
class Process {
public:
    explicit Process(const std::vector<std::string> &commands);

    ~Process();

private:
    std::unique_ptr<subprocess::Popen> process_;
};

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

}  // namespace hermes

#endif  // HERMES_PROCESS_HH
