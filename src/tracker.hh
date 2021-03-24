#ifndef HERMES_TRACKER_HH
#define HERMES_TRACKER_HH

#include "transaction.hh"

namespace hermes {

class Tracker {
public:
    explicit Tracker(std::string name): name_(std::move(name)) {}

    Transaction * get_new_transaction();
    virtual Transaction *track(Event *event) = 0;

private:
    std::string name_;
    TransactionBatch transactions_;
};

}

#endif  // HERMES_TRACKER_HH
