#include "event.hh"
#include "gtest/gtest.h"


TEST(event, event_time) {   // NOLINT
    constexpr auto event_time = 1u;
    hermes::Event e(event_time);
    EXPECT_EQ(e.time(), event_time);
}


TEST(event, event_set_get) {    // NOLINT
    hermes::Event e(0);
    e.add_value<uint8_t>("v1", 1);
    auto v1 = e.get_value<uint8_t>("v1");
    EXPECT_TRUE(v1);
    EXPECT_EQ(*v1, 1);
    e.add_value("v2", "42");
    auto v2 = e.get_value<std::string>("v2");
    EXPECT_TRUE(v2);
    EXPECT_EQ(*v2, "42");
    EXPECT_FALSE(e.add_value<uint8_t>("v2", 1));
    EXPECT_FALSE(e.get_value<uint64_t>("v3"));
}
