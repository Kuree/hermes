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

        for (auto it = start; it != end && it != event_batch->end(); it++) {
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
    if (!event_batch) return result;
    // some approximation
    result.reserve(max_time - min_time + 1);
    auto start = event_batch->lower_bound(min_time);
    auto end = event_batch->upper_bound(max_time);

    for (auto it = start; it != end && it != event_batch->end(); it++) {
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

        for (auto it = start; it != transaction_batch->end(); it++) {
            auto const &event = *it;
            if (event->start_time() > max_time) break;
            batch.emplace_back(*it);
        }
    }
    return batch;
}

TransactionBatch QueryHelper::concurrent_transactions(const std::string &transaction_name,
                                                      uint64_t min_time, uint64_t max_time) {
    auto transaction_batch = loader_->get_transactions(transaction_name, min_time, max_time);
    TransactionBatch batch;
    if (!transaction_batch) return batch;
    auto start = transaction_batch->lower_bound(min_time);

    for (auto it = start; it != transaction_batch->end(); it++) {
        auto const &event = *it;
        if (event->start_time() > max_time) break;
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

std::shared_ptr<Transaction> QueryHelper::next_transaction(
    const std::shared_ptr<Transaction> &transaction) {
    // this is actually more complicated than it looks, due to the fact that streaming
    // logic is handled differently from the query helper.
    // because transaction has no name attached to it (it's meta-data)
    // as a result, we need to figure out which batch it comes from

}

}  // namespace hermes