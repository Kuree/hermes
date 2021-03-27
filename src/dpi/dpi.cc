#include "dpi.hh"

#include "process.hh"

void DPILogger::create_events(uint64_t num_events) {
    std::lock_guard guard(events_lock_);
    events_.resize(num_events);
    for (auto &ptr : events_) ptr = std::make_shared<hermes::Event>(0);
}

void DPILogger::send_events() {
    // we dispatch it to a different threads
    auto *dispatcher = hermes::Dispatcher::get_default_dispatcher();
    dispatcher->dispatch([this]() {
        std::lock_guard guard(events_lock_);
        for (auto const &event : events_) {
            log(event);
        }
        events_.clear();
    });
}