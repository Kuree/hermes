#include <iostream>

#include "../src/checker.hh"
#include "gtest/gtest.h"
#include "test_util.hh"

class CheckerTest : public ::testing::Test, public EventTransactionInitializer {
    void SetUp() override {
        num_transaction_batch = 16;
        setup();
    }
};

class Checker1 : public hermes::Checker {
public:
    void check(const hermes::TransactionData &transaction_data,
               const std::shared_ptr<hermes::QueryHelper> &) override {
        // do nothing, i.e. everything is correct
    }
};

TEST_F(CheckerTest, check1) {  // NOLINT
    Checker1 checker;
    checker.run(name, loader);
}

class Checker2 : public hermes::Checker {
public:
    explicit Checker2(bool exception = false) { assert_exception_ = exception; }

    void check(const hermes::TransactionData &transaction_data,
               const std::shared_ptr<hermes::QueryHelper> &) override {
        // do nothing, i.e. everything is correct
        auto const &events = transaction_data.events;
        assert_(events->size() != 10, "Events size mismatch");
    }
};

TEST_F(CheckerTest, check2_cerr) {  // NOLINT
    Checker2 checker;
    std::stringstream buffer;
    auto *buf = std::cerr.rdbuf();
    std::cerr.rdbuf(buffer.rdbuf());

    checker.run(name, loader);
    // restore
    std::cerr.rdbuf(buf);
    auto str = buffer.str();
    EXPECT_NE(str.find("ERROR"), std::string::npos);
}

TEST_F(CheckerTest, check2_except) {  // NOLINT
    Checker2 checker(true);
    EXPECT_THROW(checker.run(name, loader), hermes::CheckerAssertion);
}
