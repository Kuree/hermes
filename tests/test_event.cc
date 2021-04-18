#include <fstream>

#include "arrow.hh"
#include "event.hh"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "loader.hh"
#include "serializer.hh"
#include "test_util.hh"

TEST(event, event_time) {  // NOLINT
    constexpr auto event_time = 1u;
    hermes::Event e(event_time);
    EXPECT_EQ(e.time(), event_time);
}

TEST(event, event_set_get) {  // NOLINT
    hermes::Event e(0);
    e.add_value<uint8_t>("v1", 1);
    auto v1 = e.get_value<uint8_t>("v1");
    EXPECT_TRUE(v1);
    EXPECT_EQ(*v1, 1);
    e.add_value("v2", "42");
    auto v2 = e.get_value<std::string>("v2");
    EXPECT_TRUE(v2);
    EXPECT_EQ(*v2, "42");
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

TEST(event_batch, where) {  // NOLINT
    hermes::EventBatch batch;

    auto constexpr num_events = 100;
    for (auto i = 0; i < num_events; i++) {
        auto event = std::make_unique<hermes::Event>(i);
        event->add_value<uint64_t>("value", i);
        batch.emplace_back(std::move(event));
    }

    auto res = batch.where([](const auto &e) -> bool { return e->time() < 42; });

    EXPECT_EQ(res->size(), 42);
}

TEST(event, log_parsing) {  // NOLINT
    TempDirectory temp;
    auto serializer = std::make_shared<hermes::Serializer>(temp.path());

    auto dummy = std::make_shared<hermes::DummyEventSerializer>();
    dummy->connect(serializer);
    auto path = fs::path(temp.path()) / "test.log";

    // need to generate some fake log files
    std::ofstream stream(path);
    for (int i = 0; i < 2000; i++) {
        stream << fmt::format("@{0}: CMD: {1} VALUE: {2}", i, i, "AAA") << std::endl;
    }
    stream.close();
    // readout the stream
    auto r = hermes::parse_event_log_fmt(path, "test", R"(@%t: CMD: %d VALUE: %s)",
                                         {"time", "cmd", "value"});
    EXPECT_TRUE(r);

    dummy->stop();
    serializer->finalize();

    // make sure we have them stored
    hermes::Loader loader(temp.path());
    auto event_batch = loader.get_events("test", 0, 2000);
    EXPECT_EQ(event_batch->size(), 2000);
    auto const &event = (*event_batch)[42];
    EXPECT_EQ(event->time(), 42);
    EXPECT_EQ(*event->get_value<std::string>("value"), "AAA");
}
