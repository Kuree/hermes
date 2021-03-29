#ifndef HERMES_PROCESS_HH
#define HERMES_PROCESS_HH

#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
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
    void wait();

    ~Process();

private:
    std::unique_ptr<subprocess::Popen> process_;
};

class Dispatcher {
public:
    void dispatch(const std::function<void()> &task);

    static Dispatcher *get_default_dispatcher() {
        static Dispatcher instance;
        return &instance;
    }

    void finish();

    ~Dispatcher() { finish(); }

private:
    std::mutex threads_lock_;
    std::queue<std::thread> threads_;

    void clean_up();
};

}  // namespace hermes

#endif  // HERMES_PROCESS_HH
