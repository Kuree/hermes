#include "event.hh"
#include "arrow.hh"
#include "gtest/gtest.h"

#include "arrow/api.h"


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


TEST(event_batch, serilizattion) {  // NOLINT
    // create random events
    hermes::EventBatch batch;
    auto constexpr num_events = 100;
    for (auto i = 0; i < num_events; i++) {
        auto event = std::make_unique<hermes::Event>(i);
        event->add_value("str", "this is str " + std::to_string(i));
        event->add_value<uint16_t>("uint16_t", 42 + i);
        event->add_value<uint32_t>("uint32_t", 43 + i);
        batch.emplace_back(std::move(event));
    }
    EXPECT_TRUE(batch.validate());

    auto [record, schema] = batch.serialize();
    auto r = hermes::serialize(record, schema);
    EXPECT_TRUE(r);

    auto table = hermes::deserialize(r);
    auto new_batch_ = hermes::EventBatch::deserialize(table);
    auto const &new_batch = *new_batch_;
    EXPECT_EQ(new_batch.size(), batch.size());
    auto const &event = new_batch[42];
    EXPECT_EQ(event->time(), 42);
    EXPECT_EQ(*event->get_value<std::string>("str"), "this is str 42");
    EXPECT_EQ(*event->get_value<uint16_t>("uint16_t"), 42 + 42);
    EXPECT_EQ(*event->get_value<uint32_t>("uint32_t"), 43 + 42);
}
