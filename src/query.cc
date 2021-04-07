#include "query.hh"

namespace hermes {

EventBatch QueryHelper::concurrent_events(uint64_t time) { return concurrent_events(time, time); }

EventBatch QueryHelper::concurrent_events(uint64_t min_time, uint64_t max_time) {
    auto event_batches = loader_->get_events(min_time, max_time);
    EventBatch result;
    // some approximation
    result.reserve(event_batches.size());
    for (auto const &event_batch : event_batches) {
        auto start = event_batch->lower_bound(min_time);
        auto end = event_batch->upper_bound(max_time);
        if (end != event_batch->end()) end++;

        for (auto it = start; it != end; it++) {
            auto const &event = *it;
            result.emplace_back(event);
        }
    }

    return result;
}

EventBatch QueryHelper::concurrent_events(const std::string &event_name, uint64_t min_time,
                                          uint64_t max_time) {
    auto event_batch = loader_->get_events(event_name, min_time, max_time);
    EventBatch result;
    // some approximation
    result.reserve(max_time - min_time + 1);
    auto start = event_batch->lower_bound(min_time);
    auto end = event_batch->upper_bound(max_time);
    if (end != event_batch->end()) end++;

    for (auto it = start; it != end; it++) {
        auto const &event = *it;
        result.emplace_back(event);
    }

    return result;
}

EventBatch QueryHelper::concurrent_events(const std::string &event_name,
                                          const std::shared_ptr<Event> &event) {
    return concurrent_events(event_name, event->time(), event->time());
}

EventBatch QueryHelper::concurrent_events(const std::shared_ptr<Event> &event) {
    return concurrent_events(event->time());
}

TransactionBatch QueryHelper::concurrent_transactions(uint64_t time) {
    return concurrent_transactions(time, time);
}

TransactionBatch QueryHelper::concurrent_transactions(uint64_t min_time, uint64_t max_time) {
    auto transaction_batches = loader_->get_transactions(min_time, max_time);
    TransactionBatch batch;
    for (auto const &transaction_batch : transaction_batches) {
        auto start = transaction_batch->lower_bound(min_time);
        auto end = transaction_batch->upper_bound(max_time);
        if (end != transaction_batch->end()) end++;

        for (auto it = start; it != end; it++) {
            batch.emplace_back(*it);
        }
    }
    return batch;
}

TransactionBatch QueryHelper::concurrent_transactions(const std::string &transaction_name,
                                                      uint64_t min_time, uint64_t max_time) {
    auto transaction_batch = loader_->get_transactions(transaction_name, min_time, max_time);
    TransactionBatch batch;
    auto start = transaction_batch->lower_bound(min_time);
    auto end = transaction_batch->upper_bound(max_time);
    if (end != transaction_batch->end()) end++;

    for (auto it = start; it != end; it++) {
        batch.emplace_back(*it);
    }

    return batch;
}

TransactionBatch QueryHelper::concurrent_transactions(
    const std::shared_ptr<Transaction> &transaction) {
    return concurrent_transactions(transaction->start_time(), transaction->end_time());
}

TransactionBatch QueryHelper::concurrent_transactions(
    const std::string &transaction_name, const std::shared_ptr<Transaction> &transaction) {
    return concurrent_transactions(transaction_name, transaction->start_time(),
                                   transaction->end_time());
}

}  // namespace hermes