#include "process.hh"

#include "subprocess.hpp"

namespace hermes {
Process::Process(const std::vector<std::string>& commands) {
    process_ = std::make_unique<subprocess::Popen>(commands);
}

Process::~Process() { process_->kill(); }

void Dispatcher::dispatch(const std::function<void()>& task) {
    threads_.emplace(std::thread([task, this]() {
        clean_up();
        task();
    }));
}

void Dispatcher::clean_up() {
    while (!threads_.empty()) {
        if (threads_.front().joinable()) {
            threads_.front().join();
            threads_.pop();
        } else {
            break;
        }
    }
}

Dispatcher::~Dispatcher() {
    while (!threads_.empty()) {
        threads_.front().join();
        threads_.pop();
    }
}

}  // namespace hermes
