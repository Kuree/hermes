#include "gtest/gtest.h"
#include "pubsub.hh"

TEST(pubsub, sub_sorting) { // NOLINT
    auto *bus = hermes::MessageBus::default_bus();
    auto sub1 = std::make_shared<hermes::Subscriber>();
    sub1->set_priority(10);

    auto sub2 = std::make_shared<hermes::Subscriber>();
    sub2->set_priority(10);

    sub1->subscribe(bus, "a");
    sub1->subscribe(bus, "a");
    sub2->subscribe(bus, "a");

    auto const *subs = bus->get_subscribers("a");
    EXPECT_EQ(subs->size(), 2);

    auto sub3 = std::make_shared<hermes::Subscriber>();
    sub3->set_priority(100);
    sub3->subscribe(bus, "a");

    subs = bus->get_subscribers("a");
    EXPECT_EQ(subs->size(), 3);

    auto v = 0u;
    for (auto const &sub: *subs) {
        EXPECT_GE(sub->priority(), v);
        v = sub->priority();
    }

}