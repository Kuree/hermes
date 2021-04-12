#ifndef HERMES_QUERY_HH
#define HERMES_QUERY_HH

#include "loader.hh"

namespace hermes {
class QueryHelper {
public:
    explicit QueryHelper(std::shared_ptr<Loader> loader) : loader_(std::move(loader)) {}

    EventBatch concurrent_events(uint64_t time);
    EventBatch concurrent_events(uint64_t min_time, uint64_t max_time);
    EventBatch concurrent_events(const std::string &event_name, uint64_t min_time,
                                 uint64_t max_time);
    EventBatch concurrent_events(const std::shared_ptr<Event> &event);
    EventBatch concurrent_events(const std::string &event_name,
                                 const std::shared_ptr<Event> &event);

    TransactionBatch concurrent_transactions(uint64_t time);
    TransactionBatch concurrent_transactions(uint64_t min_time, uint64_t max_time);
    TransactionBatch concurrent_transactions(const std::string &transaction_name, uint64_t min_time,
                                            uint64_t max_time);
    TransactionBatch concurrent_transactions(const std::shared_ptr<Transaction> &transaction);
    TransactionBatch concurrent_transactions(const std::string &transaction_name,
                                             const std::shared_ptr<Transaction> &transaction);

    std::shared_ptr<Transaction> next_transaction(const std::shared_ptr<Transaction> &transaction);

    std::shared_ptr<Loader> get_loader() { return loader_; }

private:
    std::shared_ptr<Loader> loader_;
};

}  // namespace hermes

#endif  // HERMES_QUERY_HH
