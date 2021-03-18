#include "transaction.hh"
#include "gtest/gtest.h"

#include "arrow/api.h"

TEST(transaction, serialization) {  // NOLINT
    hermes::TransactionBatch batch;
    constexpr auto num_transactions = 100;
    batch.reserve(num_transactions);
    uint64_t time = 0;
    for (auto i = 0; i < num_transactions; i++) {
        auto num_events = i % 10;
        auto transaction = std::make_unique<hermes::Transaction>(i);
        for (auto j = 0; j < num_events; j++) {
            auto e = std::make_unique<hermes::Event>(time++);
            transaction->add_event(e);
        }
        batch.emplace_back(std::move(transaction));
    }

    auto buffer_allocator = [](uint64_t size) -> std::shared_ptr<arrow::Buffer> {
      auto r = arrow::AllocateBuffer(static_cast<int64_t>(size));
      if (!r.ok()) {
          return nullptr;
      } else {
          std::shared_ptr<arrow::Buffer> ptr = std::move(*r);
          return ptr;
      }
    };

    auto buffer = batch.serialize(buffer_allocator);

    auto new_batch_ptr = hermes::TransactionBatch::deserialize(buffer);
    auto &new_batch = *new_batch_ptr;
    EXPECT_EQ(new_batch.size(), batch.size());
}