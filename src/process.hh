#ifndef HERMES_PROCESS_HH
#define HERMES_PROCESS_HH

#include <cstdio>
#include <memory>
#include <string>
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
}  // namespace hermes

#endif  // HERMES_PROCESS_HH
