#include "gtest/gtest.h"
#include "rtl.hh"
#include "test_util.hh"

TEST(rtl, load_enum) {  // NOLINT
    auto filename = get_vector_path("test_enum.sv");
    EXPECT_TRUE(fs::exists(filename));
    auto rtl = hermes::RTL(filename);
    EXPECT_FALSE(rtl.has_error());

    auto A = rtl.get("A");
    EXPECT_TRUE(A);
    EXPECT_EQ(*A, 0);
    auto error = rtl.get("AA");
    EXPECT_FALSE(error);

    auto pkg_opt = rtl.package("test_pkg");
    EXPECT_TRUE(pkg_opt);
    EXPECT_FALSE(rtl.package("pkg"));
    auto pkg = *pkg_opt;
    auto F = pkg.get("F");
    EXPECT_EQ(*F, 2);
}

TEST(rtl, enum_lookup) {  // NOLINT
    auto filename = get_vector_path("test_enum.sv");
    EXPECT_TRUE(fs::exists(filename));
    auto rtl = hermes::RTL(filename);
    EXPECT_FALSE(rtl.has_error());

    auto name = rtl.lookup(0);
    EXPECT_TRUE(name);
    EXPECT_TRUE(*name == "A" || *name == "C");

    name = rtl.lookup(1, "ENUM_2");
    EXPECT_TRUE(name);
    EXPECT_EQ(*name, "D");

    name = rtl.lookup(3, "test_pkg", "ENUM_2");
    EXPECT_TRUE(name);
    EXPECT_EQ(*name, "G");

    name = rtl.lookup(3, "test_pkg", "ENUM_1");
    EXPECT_FALSE(name);
}