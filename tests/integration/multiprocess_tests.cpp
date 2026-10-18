// Cross-process tests over real SysV shared memory.
#include "process_utils.h"
#include "shm_test_support.h"

#include <catch2/catch_test_macros.hpp>

using namespace mcp::core;
using namespace shmctx_test;

namespace {
constexpr int64_t kQueue = 1 << 20;   // 1 MiB storage
constexpr int64_t kHint = 32;         // -> 32768 ledger slots

// Child body: attach and publish `count` packets tagged with `producer`.
int ProducerChild(const std::string& key, uint32_t producer, uint32_t count, uint16_t topic = 0)
{
  ShmContext ctx(key, true);
  if (ctx.Attach() != 0) return 2;
  PacketProducer prod(ctx, Packet::kSeqOffset, Packet::kTsOffset);
  for (uint32_t i = 0; i < count; ++i) {
    Packet p;
    p.Stamp(producer, i);
    if (prod.ProduceWithTopic(p, topic) != 0) return 3;
  }
  return 0;
}

// Drain `expected` packets in the calling process; verify checksums and
// per-producer ordering. Returns number received.
uint32_t Drain(ShmConsumer& consumer, uint32_t expected, std::chrono::milliseconds timeout, bool& ok)
{
  std::vector<uint32_t> next(64, 0);
  uint32_t got = 0;
  ok = true;
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (got < expected && std::chrono::steady_clock::now() < deadline) {
    auto buf = consumer.TryConsume();
    if (buf.data == nullptr) { std::this_thread::yield(); continue; }
    const Packet& p = As<Packet>(buf);
    if (!p.Valid() || p.producer >= 64 || p.index != next[p.producer]) ok = false;
    else ++next[p.producer];
    ++got;
  }
  return got;
}
} // namespace

TEST_CASE("Child process produces, parent consumes", "[multiprocess]")
{
  ScopedShmKeyFile key;
  ShmContext ctx(key.path(), true);
  REQUIRE(ctx.Create(kQueue, 2, kHint) == 0);
  ShmConsumer consumer(ctx);   // cursor fixed before any child publishes

  constexpr uint32_t kCount = 10000;
  pid_t child = ForkChild([&] { return ProducerChild(key.path(), 1, kCount); });

  bool ok = false;
  uint32_t got = Drain(consumer, kCount, std::chrono::seconds(20), ok);
  CHECK(WaitChild(child) == 0);
  CHECK(got == kCount);
  CHECK(ok);
  CHECK(Committed(ctx) == kCount);
  CHECK(ctx.Destroy() == 0);
}

TEST_CASE("Three producer processes interleave into one ordered stream", "[multiprocess]")
{
  ScopedShmKeyFile key;
  ShmContext ctx(key.path(), true);
  REQUIRE(ctx.Create(kQueue, 2, kHint) == 0);
  ShmConsumer consumer(ctx);

  constexpr uint32_t kPer = 5000;
  constexpr int kProcs = 3;
  std::vector<pid_t> kids;
  for (int p = 0; p < kProcs; ++p)
    kids.push_back(ForkChild([&, p] { return ProducerChild(key.path(), uint32_t(p), kPer); }));

  bool ok = false;
  uint32_t got = Drain(consumer, kPer * kProcs, std::chrono::seconds(30), ok);
  for (pid_t k : kids) CHECK(WaitChild(k) == 0);
  CHECK(got == kPer * kProcs);
  CHECK(ok);

  // Every committed slot carries a unique sequence number 1..N.
  CHECK(Committed(ctx) == kPer * kProcs);
  CHECK(Claimed(ctx) == kPer * kProcs);
  CHECK(ctx.Destroy() == 0);
}

TEST_CASE("Producer and consumer in separate child processes", "[multiprocess]")
{
  ScopedShmKeyFile key;
  ShmContext ctx(key.path(), true);
  REQUIRE(ctx.Create(kQueue, 2, kHint) == 0);

  constexpr uint32_t kCount = 20000;
  ReadyPipe ready;

  // Consumer child: attach, register cursor, signal ready, then drain.
  pid_t consumer_pid = ForkChild([&] {
    ShmContext c(key.path(), true);
    if (c.Attach() != 0) return 2;
    ShmConsumer consumer(c);
    ready.SignalReady();
    bool ok = false;
    uint32_t got = Drain(consumer, kCount, std::chrono::seconds(20), ok);
    if (got != kCount) return 4;
    return ok ? 0 : 5;
  });
  REQUIRE(ready.WaitReady());

  pid_t producer_pid = ForkChild([&] { return ProducerChild(key.path(), 7, kCount, 3); });

  CHECK(WaitChild(producer_pid) == 0);
  CHECK(WaitChild(consumer_pid) == 0);
  CHECK(ctx.Destroy() == 0);
}

TEST_CASE("Blocking Consume() in a child wakes on a parent publish", "[multiprocess][blocking]")
{
  ScopedShmKeyFile key;
  ShmContext ctx(key.path(), true);
  REQUIRE(ctx.Create(kQueue, 2, kHint) == 0);
  ReadyPipe ready;

  pid_t consumer_pid = ForkChild([&] {
    ShmContext c(key.path(), true);
    if (c.Attach() != 0) return 2;
    ShmConsumer consumer(c);
    ready.SignalReady();
    auto buf = consumer.Consume();   // spins until the parent publishes
    const Packet& p = As<Packet>(buf);
    return (p.Valid() && p.index == 4242) ? 0 : 6;
  });
  REQUIRE(ready.WaitReady());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));   // make sure it is actually blocked

  ShmProducer producer(ctx);
  Packet p;
  p.Stamp(0, 4242);
  REQUIRE(producer.Produce(p) == 0);

  CHECK(WaitChild(consumer_pid, std::chrono::seconds(10)) == 0);
  CHECK(ctx.Destroy() == 0);
}

TEST_CASE("Topic-filtered consumer in a child only receives its topic", "[multiprocess][topics]")
{
  ScopedShmKeyFile key;
  ShmContext ctx(key.path(), true);
  REQUIRE(ctx.Create(kQueue, 2, kHint) == 0);
  ReadyPipe ready;
  constexpr uint32_t kPer = 3000;

  pid_t consumer_pid = ForkChild([&] {
    ShmContext c(key.path(), true);
    if (c.Attach() != 0) return 2;
    ShmConsumer consumer(c);
    ready.SignalReady();
    SubscribedTopics subs;
    subs.set(2);
    uint32_t got = 0, expect = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (got < kPer && std::chrono::steady_clock::now() < deadline) {
      auto buf = consumer.TryConsume(subs);
      if (buf.data == nullptr) continue;
      if (buf.topic != 2) return 7;
      const Packet& p = As<Packet>(buf);
      if (p.producer != 2 || p.index != expect++) return 8;
      ++got;
    }
    return got == kPer ? 0 : 9;
  });
  REQUIRE(ready.WaitReady());

  // Producer N publishes on topic N.
  std::vector<pid_t> kids;
  for (uint16_t t = 1; t <= 3; ++t)
    kids.push_back(ForkChild([&, t] { return ProducerChild(key.path(), t, kPer, t); }));
  for (pid_t k : kids) CHECK(WaitChild(k) == 0);
  CHECK(WaitChild(consumer_pid) == 0);
  CHECK(ctx.Destroy() == 0);
}

TEST_CASE("ShmAdmin in another process sees the live sequence numbers", "[multiprocess][admin]")
{
  ScopedShmKeyFile key;
  ShmContext ctx(key.path(), true);
  REQUIRE(ctx.Create(kQueue, 2, kHint) == 0);
  ShmProducer producer(ctx);
  int v = 0;
  for (int i = 0; i < 123; ++i) REQUIRE(producer.Produce(v) == 0);

  pid_t admin_pid = ForkChild([&] {
    ShmContext c(key.path(), true);
    if (c.Attach() != 0) return 2;
    ShmAdmin admin(c);
    if (admin.GetCurrentSequenceNumber() != 123) return 10;
    if (admin.GetClaimedSequenceNumber() != 123) return 11;
    std::vector<ConsumerStats> stats;
    admin.GetConsumerStats(stats);
    return stats.size() == 2 ? 0 : 12;
  });
  CHECK(WaitChild(admin_pid) == 0);
  CHECK(ctx.Destroy() == 0);
}
