#include "dpi.hh"

#include <filesystem>

#include "process.hh"
#include "serializer.hh"

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

DPILogger::~DPILogger() {
    // no multi-threading
    if (!events_.empty()) {
        std::lock_guard guard(events_lock_);
        for (auto const &event : events_) {
            log(event);
        }
        events_.clear();
    }
}

// global variables
std::vector<DPILogger *> loggers;
std::string serializer_path;
hermes::Serializer *serializer = nullptr;

[[maybe_unused]] void hermes_set_output_dir(const char *directory) { serializer_path = directory; }

[[maybe_unused]] void *hermes_create_logger(const char *name) {
    if (!serializer) {
        // initialize the serializer
        if (serializer_path.empty()) {
            // use the current working directory
            serializer_path = std::filesystem::current_path();
        }
        serializer = new hermes::Serializer(serializer_path);
    }

    auto *logger = new DPILogger(name);
    loggers.emplace_back(logger);
    return logger;
}

DPILogger *get_logger(void *logger) { return reinterpret_cast<DPILogger *>(logger); }

void hermes_create_events(void *logger, uint64_t num_events) {
    auto *l = get_logger(logger);
    l->create_events(num_events);
}

template <typename T>
T *get_pointer(svOpenArrayHandle array, int index) {
    return reinterpret_cast<T *>(svGetArrElemPtr1(array, index));
}

void hermes_set_values_uint8(void *logger, const char *name, svOpenArrayHandle array) {
    auto *l = get_logger(logger);
    auto low = svRight(array, 1);
    auto high = svLeft(array, 1);
    for (auto i = low; i < high; i++) {
        auto *v = get_pointer<uint8_t>(array, i);
        l->set_value(name, *v, static_cast<uint64_t>(i));
    }
}

void hermes_set_values_uint16(void *logger, const char *name, svOpenArrayHandle array) {
    auto *l = get_logger(logger);
    auto low = svRight(array, 1);
    auto high = svLeft(array, 1);
    for (auto i = low; i < high; i++) {
        auto *v = get_pointer<uint16_t>(array, i);
        l->set_value(name, *v, static_cast<uint64_t>(i));
    }
}

void hermes_set_values_uin32(void *logger, const char *name, svOpenArrayHandle array) {
    auto *l = get_logger(logger);
    auto low = svRight(array, 1);
    auto high = svLeft(array, 1);
    for (auto i = low; i < high; i++) {
        auto *v = get_pointer<uint32_t>(array, i);
        l->set_value(name, *v, static_cast<uint64_t>(i));
    }
}

void hermes_set_values_uint64(void *logger, const char *name, svOpenArrayHandle array) {
    auto *l = get_logger(logger);
    auto low = svRight(array, 1);
    auto high = svLeft(array, 1);
    for (auto i = low; i < high; i++) {
        auto *v = get_pointer<uint64_t>(array, i);
        l->set_value(name, *v, static_cast<uint64_t>(i));
    }
}

void hermes_set_values_string(void *logger, const char *name, svOpenArrayHandle array) {
    auto *l = get_logger(logger);
    auto low = svRight(array, 1);
    auto high = svLeft(array, 1);
    for (auto i = low; i < high; i++) {
        auto *v = get_pointer<char *>(array, i);
        l->set_value(name, *v, static_cast<uint64_t>(i));
    }
}

void hermes_send_events(void *logger) {
    auto *l = get_logger(logger);
    l->send_events();
}

void hermes_final() {
    for (auto *ptr : loggers) {
        delete ptr;
    }
    // need to flush the subscriber as well, if any
    auto *bus = hermes::MessageBus::default_bus();
    auto subs = bus->get_subscribers();
    for (const auto &sub: subs) {
        sub->stop();
    }
    delete serializer;
}
