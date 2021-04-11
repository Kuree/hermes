#ifndef HERMES_CHECKER_HH
#define HERMES_CHECKER_HH

#include "transaction.hh"
#include "loader.hh"


namespace hermes {

class Checker {
public:
    Checker() = default;
    virtual void check(const std::shared_ptr<Transaction> &transaction) = 0;

    void run(const std::string &transaction_name, const std::shared_ptr<Loader> &loader);

    [[nodiscard]] bool stateless() const { return stateless_; }
    void set_stateless(bool value) { stateless_ = value; }

private:
    bool stateless_ = true;
};

}  // namespace hermes

#endif  // HERMES_CHECKER_HH