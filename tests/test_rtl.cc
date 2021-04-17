#include "gtest/gtest.h"
#include "test_util.hh"
#include "rtl.hh"

TEST(rtl, load_enum) {  // NOLINT
    auto filename = get_vector_path("test_enum.sv");
    EXPECT_TRUE(fs::exists(filename));
    auto rtl = hermes::RTL(filename);
    EXPECT_FALSE(rtl.has_error());
}