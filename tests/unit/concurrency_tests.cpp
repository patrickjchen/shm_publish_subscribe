// Multi-threaded producers / consumers sharing one in-process context.
#include "shm_test_support.h"

#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <map>
#include <thread>

using namespace mcp::core;
using namespace shmctx_test;

namespace {
constexpr int kProducers = 4;
constexpr uint32_t kPerProducer = 5000;

// Ledger large enough that nothing is overwritten before the consumer reads it:
// storage 4 MiB / size_hint 32 -> 131072 ledger slots > 20000 messages.
struct BigCtx
{
  ShmContext ctx{ "x", false };
  BigCtx() { REQUIRE(ctx.Create(1 << 22, 4, 32) == 0); }
};
} // namespace

TEST_CASE("Multiple producer threads: every message arrives once, per-producer order kept", "[concurrency][producers]")
{
  BigCtx c;
  ShmConsumer consumer(c.ctx);

  std::atomic<bool> go{ false };
  std::vector<std::thread> threads;
  for (int t = 0; t < kProducers; ++t) {
    threads.emplace_back([&, t] {
      ShmProducer producer(c.ctx);
      while (!go.load()) {}
      for (uint32_t i = 0; i < kPerProducer; ++i) {
        Packet p;
        p.Stamp(uint32_t(t), i);
        producer.Produce(p);
      }
    });
  }
  go = true;

  std::vector<uint32_t> next(kProducers, 0);
  uint32_t total = 0;
  const uint32_t expected = kProducers * kPerProducer;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (total < expected) {
    auto buf = consumer.TryConsume();
    if (buf.data == nullptr) {
      REQUIRE(std::chrono::steady_clock::now() < deadline);
      continue;
    }
    const Packet& p = As<Packet>(buf);
    REQUIRE(p.Valid());
    REQUIRE(p.producer < uint32_t(kProducers));
    CHECK(p.index == next[p.producer]);
    ++next[p.producer];
    ++total;
  }
  for (auto& th : threads) th.join();

  CHECK(total == expected);
  CHECK(Committed(c.ctx) == expected);
  CHECK(Claimed(c.ctx) == expected);
  CHECK(consumer.TryConsume().data == nullptr);
}

TEST_CASE("PacketProducer across threads yields a gap-free, unique sequence", "[concurrency][seq]")
{
  BigCtx c;
  ShmConsumer consumer(c.ctx);
  std::vector<std::thread> threads;
  for (int t = 0; t < kProducers; ++t) {
    threads.emplace_back([&, t] {
      PacketProducer producer(c.ctx, Packet::kSeqOffset, Packet::kTsOffset);
      for (uint32_t i = 0; i < kPerProducer; ++i) {
        Packet p;
        p.Stamp(uint32_t(t), i);
        producer.Produce(reinterpret_cast<char*>(&p), sizeof(p));
      }
    });
  }
  for (auto& th : threads) th.join();

  const uint32_t expected = kProducers * kPerProducer;
  for (uint32_t s = 1; s <= expected; ++s) {
    auto buf = consumer.TryConsume();
    REQUIRE(buf.data != nullptr);
    // Each payload was stamped with the slot it was committed into.
    CHECK(As<Packet>(buf).seq == s);
  }
  CHECK(consumer.TryConsume().data == nullptr);
}

TEST_CASE("Multiple consumers each observe the full stream (broadcast)", "[concurrency][consumers]")
{
  BigCtx c;
  constexpr int kConsumers = 3;
  constexpr uint32_t kMessages = 20000;

  std::vector<std::unique_ptr<ShmConsumer>> consumers;
  for (int i = 0; i < kConsumers; ++i) consumers.push_back(std::make_unique<ShmConsumer>(c.ctx));

  std::vector<uint32_t> received(kConsumers, 0);
  std::vector<bool> ordered(kConsumers, true);
  std::vector<std::thread> threads;
  for (int i = 0; i < kConsumers; ++i) {
    threads.emplace_back([&, i] {
      uint32_t expect = 0;
      while (received[i] < kMessages) {
        auto buf = consumers[i]->Consume();
        const Packet& p = As<Packet>(buf);
        if (p.index != expect || !p.Valid()) ordered[i] = false;
        ++expect;
        ++received[i];
      }
    });
  }

  ShmProducer producer(c.ctx);
  for (uint32_t i = 0; i < kMessages; ++i) {
    Packet p;
    p.Stamp(0, i);
    producer.Produce(p);
  }
  for (auto& th : threads) th.join();

  for (int i = 0; i < kConsumers; ++i) {
    CHECK(received[i] == kMessages);
    CHECK(ordered[i]);
  }
}

TEST_CASE("Producer and consumer running concurrently with lagging consumer still see FIFO", "[concurrency][pipeline]")
{
  BigCtx c;
  ShmConsumer consumer(c.ctx);
  constexpr uint32_t kMessages = 50000;

  std::thread prod([&] {
    ShmProducer producer(c.ctx);
    for (uint32_t i = 0; i < kMessages; ++i) {
      Packet p;
      p.Stamp(0, i);
      producer.Produce(p);
      if (i % 1000 == 0) std::this_thread::yield();
    }
  });

  uint32_t got = 0;
  while (got < kMessages) {
    auto buf = consumer.TryConsume();
    if (buf.data == nullptr) { std::this_thread::yield(); continue; }
    REQUIRE(As<Packet>(buf).index == got);
    ++got;
  }
  prod.join();
  CHECK(got == kMessages);
}

TEST_CASE("ShmAdmin reports sequence numbers", "[concurrency][admin]")
{
  BigCtx c;
  ShmAdmin admin(c.ctx);
  ShmProducer producer(c.ctx);
  CHECK(admin.GetCurrentSequenceNumber() == 0);
  int v = 0;
  for (int i = 0; i < 10; ++i) producer.Produce(v);
  CHECK(admin.GetCurrentSequenceNumber() == 10);
  CHECK(admin.GetClaimedSequenceNumber() == 10);
  std::vector<ConsumerStats> stats;
  admin.GetConsumerStats(stats);
  CHECK(stats.size() == 4);
}
