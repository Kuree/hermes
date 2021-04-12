#include "../src/checker.hh"
#include "gtest/gtest.h"
#include "test_util.hh"

class CheckerTest : public ::testing::Test, public EventTransactionInitializer {
    void SetUp() override { setup(); }
};

class Checker1 : public hermes::Checker {
public:
    void check(const std::shared_ptr<hermes::Transaction> &,
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

    void check(const std::shared_ptr<hermes::Transaction> &transaction,
               const std::shared_ptr<hermes::QueryHelper> &) override {
        // do nothing, i.e. everything is correct
        auto const &events = transaction->events();
        assert_(events.size() != 10, "Events size mismatch");
    }
};

TEST_F(CheckerTest, check2_cerr) {  // NOLINT
    Checker2 checker;
    testing::internal::CaptureStderr();
    checker.run(name, loader);
    auto cerr = testing::internal::GetCapturedStderr();
    EXPECT_NE(cerr.find("ERROR"), std::string::npos);
}

TEST_F(CheckerTest, check2_except) {  // NOLINT
    Checker2 checker(true);
    testing::internal::CaptureStderr();
    EXPECT_THROW(checker.run(name, loader), hermes::CheckerAssertion);
}
