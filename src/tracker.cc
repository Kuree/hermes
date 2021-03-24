#include "tracker.hh"

namespace hermes {

Transaction * Tracker::get_new_transaction() {
    // we use the default id allocator
    auto t = std::make_unique<Transaction>();
    transactions_.emplace_back(std::move(t));
    return transactions_.back().get();
}

}