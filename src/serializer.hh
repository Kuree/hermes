#ifndef HERMES_SERIALIZER_HH
#define HERMES_SERIALIZER_HH

#include "event.hh"
#include "transaction.hh"
#include <mutex>

namespace hermes {
class Serializer {
public:
    explicit Serializer(std::string output_dir);

    bool serialize(const EventBatch &batch);
    bool serialize(const TransactionBatch &batch);

private:
    std::string output_dir_;
    uint64_t batch_counter_ = 0;
    std::mutex batch_mutex_;

    std::pair<std::string, std::string> get_next_filename();
};
}  // namespace hermes

#endif  // HERMES_SERIALIZER_HH
