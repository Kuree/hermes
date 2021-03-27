#ifndef HERMES_LOGGER_HH
#define HERMES_LOGGER_HH

#include "event.hh"
#include "pubsub.hh"

namespace hermes {
class Logger : Publisher {
public:
    explicit Logger(std::string name) : Publisher(), name_(std::move(name)) {}
    Logger(MessageBus *bus, std::string name) : Publisher(bus), name_(std::move(name)) {}

    void log(const std::shared_ptr<Event> &event) { publish(name_, event); }

private:
    std::string name_;
};

}  // namespace hermes

#endif  // HERMES_LOGGER_HH
