#include "process.hh"

#include "subprocess.hpp"

namespace hermes {
Process::Process(const std::vector<std::string>& commands) {
    process_ = std::make_unique<subprocess::Popen>(commands);
}

void Process::wait() {
    process_->wait();
}

Process::~Process() { process_->kill(); }

void Dispatcher::dispatch(const std::function<void()>& task) {
    std::lock_guard guard(threads_lock_);
    clean_up();
    threads_.emplace(std::thread([task, this]() {
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

void Dispatcher::finish() {
    std::lock_guard guard(threads_lock_);
    while (!threads_.empty()) {
        threads_.front().join();
        threads_.pop();
    }
}

}  // namespace hermes
