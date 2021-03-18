#include "event.hh"
#include "gtest/gtest.h"

#include "plasma/client.h"
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
    hermes::EventBatch batch("test");
    auto constexpr num_events = 42;
    for (auto i = 0; i < num_events; i++) {
        auto event = std::make_unique<hermes::Event>(i);
        event->add_value("str", "this is str");
        event->add_value<uint16_t>("uint16_t", 42);
        event->add_value<uint32_t>("uint32_t", 43);
        batch.emplace_back(std::move(event));
    }

    auto buffer_allocator = [](uint64_t size) -> std::shared_ptr<arrow::Buffer> {
        auto r = arrow::AllocateBuffer(static_cast<int64_t>(size));
        if (!r.ok()) return nullptr;
        else return std::move(*r);
    };

    auto r = batch.serialize(buffer_allocator);
    EXPECT_TRUE(r);
}
