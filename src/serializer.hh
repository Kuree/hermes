#ifndef HERMES_SERIALIZER_HH
#define HERMES_SERIALIZER_HH

#include "event.hh"
#include "transaction.hh"

namespace hermes {
class Serializer {
public:
    explicit Serializer(std::string output_dir);

    void serialize(const EventBatch &batch);
    void serialize(const TransactionBatch &batch);

private:
    std::string output_dir_;
    uint64_t batch_counter_ = 0;
};
}  // namespace hermes

#endif  // HERMES_SERIALIZER_HH
