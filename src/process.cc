#include "process.hh"

#include "subprocess.hpp"

namespace hermes {
Process::Process(const std::vector<std::string> &commands) {
    process_ = std::make_unique<subprocess::Popen>(commands);
}

Process::~Process() { process_->kill(); }
}  // namespace hermes
