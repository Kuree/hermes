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
    if (num_events > max_events_size) {
        std::cerr << "Unable to allocate events which exceeds " << max_events_size << std::endl;
    }
}

void DPILogger::send_events() {
    // we dispatch it to a different threads
    auto *dispatcher = hermes::Dispatcher::get_default_dispatcher();
    dispatcher->dispatch([this]() {
        std::lock_guard guard(events_lock_);
        for (auto const &event : events_) {
            // set event name
            event->set_name(topic_);
            log(event);
        }
        events_.clear();
    });
}

void DPILogger::add_thread(const std::function<void()> &func) {
    threads_.emplace_back(std::thread(func));
}

void DPILogger::join() {
    for (auto &t : threads_) t.join();
    threads_.clear();
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
std::vector<std::shared_ptr<DPITracker>> trackers;
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

inline DPILogger *get_logger(void *logger) { return reinterpret_cast<DPILogger *>(logger); }
inline hermes::Tracker *get_tracker(void *tracker) {
    return reinterpret_cast<hermes::Tracker *>(tracker);
}
inline hermes::Transaction *get_transaction(void *transaction) {
    return reinterpret_cast<hermes::Transaction *>(transaction);
}

inline hermes::Event *get_event(void *event) { return reinterpret_cast<hermes::Event *>(event); }

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

[[maybe_unused]] void hermes_create_events_id(void *logger, svOpenArrayHandle times,
                                              svOpenArrayHandle event_ids) {
    hermes_create_events(logger, times);
    // get events and set them
    auto *l = get_logger(logger);
    auto events = l->events();
    for (int i = 0; i < svSize(event_ids, 1); i++) {
        auto *ptr = reinterpret_cast<void **>(svGetArrElemPtr1(event_ids, i));
        *ptr = events[i].get();
    }
}

template <typename T, typename K = T>
void set_values(DPILogger *logger, svOpenArrayHandle names, svOpenArrayHandle array) {
    auto low = svLeft(array, 1);
    auto high = svRight(array, 1);
    auto num_entries = static_cast<uint64_t>(high - low + 1l);
    // sanity check on array sizes
    auto names_low = svLeft(names, 1);
    auto names_high = svRight(names, 1);
    auto num_entries_names = static_cast<uint64_t>(names_high - names_low + 1l);
    if ((num_entries % num_entries_names) != 0) {
        // frontend does not implement the logic correctly
        std::cerr << "[ERROR]: log values are not multiple of entry names. Expected "
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
    auto const entries_per_event = num_entries / logger->num_events();

    // allocate space
    std::vector<std::string> names_vector;
    names_vector.reserve(num_entries_names);
    for (auto i = 0; i < num_entries_names; i++) {
        names_vector.emplace_back(*get_pointer<const char *>(names, static_cast<int>(i)));
    }

    std::vector<K> values_vector;
    values_vector.reserve(num_entries);
    for (auto i = 0; i < num_entries; i++) {
        auto *v = get_pointer<T>(array, i);
        K value;
        if constexpr (std::is_same<T, bool>::value) {
            auto *v_ptr = reinterpret_cast<unsigned char *>(v);
            value = *v_ptr != 0;
        } else {
            value = *v;
        }
        values_vector.emplace_back(value);
    }

    // thread block
    logger->add_thread([=]() {
        uint64_t counter = 0;
        for (auto i = 0u; i < num_events; i++) {
            for (auto j = 0; j < entries_per_event; j++) {
                auto const &name = names_vector[j];
                auto v = values_vector[counter];
                logger->template set_value(name, v, i);
                counter++;
            }
        }
    });
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
    set_values<char *, std::string>(l, names, array);
}

[[maybe_unused]] void hermes_send_events(void *logger) {
    auto *l = get_logger(logger);
    l->join();
    l->send_events();
}

[[maybe_unused]] void hermes_final() {
    for (auto *ptr : loggers) {
        delete ptr;
    }
    loggers.clear();
    for (auto const &t : trackers) {
        t->stop();
    }

    trackers.clear();

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

[[maybe_unused]] void hermes_set_serializer_dir(const char *topic) {
    auto serializer = get_serializer();
    serializer->set_output_dir(topic);
}

[[maybe_unused]] void *hermes_create_tracker(const char *name) {
    auto tracker = std::make_shared<DPITracker>(name);
    trackers.emplace_back(tracker);
    auto serializer = get_serializer();
    tracker->set_serializer(serializer);
    return tracker.get();
}

[[maybe_unused]] void *hermes_tracker_new_transaction(void *tracker) {
    auto *t = get_tracker(tracker);
    return t->get_new_transaction();
}

[[maybe_unused]] void hermes_transaction_finish(void *transaction) {
    auto *t = get_transaction(transaction);
    t->finish();
}

[[maybe_unused]] void hermes_retire_transaction(void *tracker, void *transaction) {
    auto *tracker_ = get_tracker(tracker);
    auto *transaction_ = get_transaction(transaction);

    tracker_->retire_transaction(transaction_->shared_from_this());
}

[[maybe_unused]] void hermes_add_event_transaction(void *transaction, void *event) {
    auto *transaction_ = get_transaction(transaction);
    auto *event_ = get_event(event);
    transaction_->add_event(event_);
}

// implement the add tracker to simulator
namespace hermes {
void add_tracker_to_simulator(const std::shared_ptr<Tracker> &tracker) {
    auto serializer = get_serializer();
    tracker->set_serializer(serializer);
    tracker->connect();
}

}  // namespace hermes
