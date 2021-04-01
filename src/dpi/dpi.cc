#include "dpi.hh"

#include <filesystem>
#include <iostream>

#include "process.hh"
#include "serializer.hh"
#include "tracker.hh"

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
std::shared_ptr<hermes::Serializer> serializer_;

std::shared_ptr<hermes::Serializer> get_serializer() {
    if (!serializer_) {
        // initialize the serializer
        if (serializer_path.empty()) {
            // use the current working directory
            serializer_path = std::filesystem::current_path();
        }
        serializer_ = std::make_shared<hermes::Serializer>(serializer_path);
    }
    return serializer_;
}

[[maybe_unused]] void hermes_set_output_dir(const char *directory) { serializer_path = directory; }

[[maybe_unused]] void *hermes_create_logger(const char *name) {
    auto *logger = new DPILogger(name);
    loggers.emplace_back(logger);
    return logger;
}

DPILogger *get_logger(void *logger) { return reinterpret_cast<DPILogger *>(logger); }

template <typename T>
T *get_pointer(svOpenArrayHandle array, int index) {
    return reinterpret_cast<T *>(svGetArrElemPtr1(array, index));
}

[[maybe_unused]] void hermes_create_events(void *logger, svOpenArrayHandle times) {
    auto *l = get_logger(logger);
    auto low = svLeft(times, 1);
    auto high = svRight(times, 1);
    l->create_events(high - low + 1);
    // set event time
    for (auto i = low; i <= high; i++) {
        auto *v = get_pointer<uint64_t>(times, i);
        l->set_time(i, *v);
    }
}

template <typename T>
void set_values(DPILogger *logger, svOpenArrayHandle names, svOpenArrayHandle array) {
    auto low = svLeft(array, 1);
    auto high = svRight(array, 1);
    auto num_entries = static_cast<uint64_t>(high - low + 1l);
    // sanity check on array sizes
    auto names_low = svLeft(names, 1);
    auto names_high = svRight(names, 1);
    auto num_entries_names = static_cast<uint64_t>(names_high - names_low + 1l);
    if (num_entries_names != num_entries) {
        // frontend is not implement the logic correctly
        std::cerr << "[ERROR]: log names does not match with the number of values. Expected "
                  << num_entries_names << ", got " << num_entries << std::endl;
        return;
    }
    auto num_events = logger->num_events();

    if ((num_entries % num_events) != 0) {
        // something is wrong, print out error message
        std::cerr << "[ERROR]: log values is not a multiple of the number of events. Expected "
                  << logger->num_events() << ", got " << num_entries << std::endl;
        return;
    }
    auto entries_per_event = num_entries / logger->num_events();

    uint64_t counter = 0;
    for (auto i = 0u; i < num_events; i++) {
        for (auto j = 0; j < entries_per_event; j++) {
            auto **name = get_pointer<char *>(names, counter);
            auto *v = get_pointer<T>(array, counter);
            if constexpr (std::is_same<T, char *>::value) {
                std::string value = *v;
                logger->template set_value(*name, value, i);
            } else if constexpr (std::is_same<T, bool>::value) {
                auto *v_ptr = reinterpret_cast<unsigned char *>(v);
                bool value = *v_ptr != 0;
                logger->template set_value(*name, value, i);
            } else {
                logger->template set_value(*name, *v, i);
            }
            counter++;
        }
    }
}

[[maybe_unused]] void hermes_set_values_uint8(void *logger, svOpenArrayHandle names,
                                              svOpenArrayHandle array) {
    auto *l = get_logger(logger);
    set_values<uint8_t>(l, names, array);
}

[[maybe_unused]] void hermes_set_values_uint16(void *logger, svOpenArrayHandle names,
                                               svOpenArrayHandle array) {
    auto *l = get_logger(logger);
    set_values<uint16_t>(l, names, array);
}

[[maybe_unused]] void hermes_set_values_uint32(void *logger, svOpenArrayHandle names,
                                               svOpenArrayHandle array) {
    auto *l = get_logger(logger);
    set_values<uint32_t>(l, names, array);
}

[[maybe_unused]] void hermes_set_values_uint64(void *logger, svOpenArrayHandle names,
                                               svOpenArrayHandle array) {
    auto *l = get_logger(logger);
    set_values<uint64_t>(l, names, array);
}

[[maybe_unused]] void hermes_set_values_bool(void *logger, svOpenArrayHandle names,
                                             svOpenArrayHandle array) {
    auto *l = get_logger(logger);
    set_values<bool>(l, names, array);
}

[[maybe_unused]] void hermes_set_values_string(void *logger, svOpenArrayHandle names,
                                               svOpenArrayHandle array) {
    auto *l = get_logger(logger);
    set_values<char *>(l, names, array);
}

[[maybe_unused]] void hermes_send_events(void *logger) {
    auto *l = get_logger(logger);
    l->send_events();
}

[[maybe_unused]] void hermes_final() {
    for (auto *ptr : loggers) {
        delete ptr;
    }
    loggers.clear();

    // need to flush the subscriber as well, if any
    auto *bus = hermes::MessageBus::default_bus();
    bus->stop();

    serializer_.reset();
}

[[maybe_unused]] void hermes_add_dummy_serializer(const char *topic) {
    auto serializer = get_serializer();
    auto p = std::make_shared<hermes::DummyEventSerializer>(topic);
    p->connect(serializer);
}

// implement the add tracker to simulator
namespace hermes {
void add_tracker_to_simulator(const std::shared_ptr<Tracker> &tracker) {
    auto serializer = get_serializer();
    tracker->set_serializer(serializer);
    tracker->connect();
}

}  // namespace hermes
