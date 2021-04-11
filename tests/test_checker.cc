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


TEST_F(CheckerTest, check1) {   // NOLINT
    Checker1 checker;
    checker.run(name, loader);
}