#ifndef HERMES_PROCESS_HH
#define HERMES_PROCESS_HH

#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <queue>
#include <thread>
#include <functional>

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


class Dispatcher {
public:
    void dispatch(const std::function<void()>& task);

    ~Dispatcher();

private:
    std::queue<std::thread> threads_;

    void clean_up();
};

}  // namespace hermes

#endif  // HERMES_PROCESS_HH
