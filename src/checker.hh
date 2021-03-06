#ifndef HERMES_CHECKER_HH
#define HERMES_CHECKER_HH

#include <exception>
#include <mutex>

#include "loader.hh"
#include "query.hh"
#include "transaction.hh"

namespace hermes {

class Checker {
public:
    Checker() = default;
    virtual void check(const hermes::TransactionData &transaction_data,
                       const std::shared_ptr<QueryHelper> &query) = 0;

    void run(const std::string &transaction_name, const std::shared_ptr<Loader> &loader);

    [[nodiscard]] bool stateless() const { return stateless_; }
    void set_stateless(bool value) { stateless_ = value; }
    [[nodiscard]] bool assert_exception() const { return assert_exception_; }
    void set_assert_exception(bool value) { assert_exception_ = value; }

    void assert_(bool value) const { assert_(value, ""); }
    void assert_(bool value, const std::string &message) const;

protected:
    bool stateless_ = true;
    bool assert_exception_ = false;
    std::mutex assert_mutex_;
    std::optional<std::exception_ptr> current_ptr_;
};

class CheckerAssertion : public std::runtime_error {
public:
    explicit CheckerAssertion(std::string msg) : std::runtime_error(std::move(msg)) {}  // NOLINT
};

}  // namespace hermes

#endif  // HERMES_CHECKER_HH
