// Producer crash recovery: a child claims a slot and dies before committing.
// Exercises the two-tier recovery in ShmContext::WaitForCommit.
//
// Requires SHMCTX_ENABLE_TEST_HOOKS so ShmContextTestHooks can reach the
// private claim/stamp primitives and stop half-way, exactly where a crash
// would leave real state behind.
#include "process_utils.h"
#include "shm_test_support.h"

#include <catch2/catch_test_macros.hpp>

namespace mcp::core {
struct ShmContextTestHooks
{
  // Phase 1 only: CAS-bump claimed_sequence_num. Mimics dying in the window
  // between the CAS and StampClaim (no pid visible to recoverers).
  static int64_t ClaimOnly(ShmContext& c)
  {
    auto expected = c.header_->claimed_sequence_num.load(std::memory_order_acquire);
    auto desired = expected + 1;
    while (!c.header_->claimed_sequence_num.compare_exchange_weak(expected, desired, std::memory_order_acq_rel))
      desired = expected + 1;
    return desired;
  }
  // Phase 1 + pid stamp: mimics dying after StampClaim but before commit.
  static int64_t ClaimAndStamp(ShmContext& c)
  {
    auto seq = ClaimOnly(c);
    c.StampClaim(seq);
    return seq;
  }
};
} // namespace mcp::core

using namespace mcp::core;
using namespace shmctx_test;

namespace {
constexpr int64_t kQueue = 1 << 16;
constexpr int64_t kHint = 32;

// Spawn a child that claims (and optionally stamps) then dies. Returns the
// claimed sequence number through `claimed`. The child must be fully reaped
// before returning: kill(pid, 0) succeeds on a zombie, and the Tier-1 probe
// relies on ESRCH.
void RunRogue(const std::string& key, bool stamp, int64_t expected_claim)
{
  pid_t pid = ForkChild([&] {
    ShmContext c(key, true);
    if (c.Attach() != 0) return 2;
    int64_t seq = stamp ? ShmContextTestHooks::ClaimAndStamp(c) : ShmContextTestHooks::ClaimOnly(c);
    return seq == expected_claim ? 0 : 20;
  });
  REQUIRE(WaitChild(pid) == 0);
}
} // namespace

TEST_CASE("Tier-1: stamped dead producer is skipped and the queue drains", "[recovery][tier1]")
{
  ScopedShmKeyFile key;
  ShmContext ctx(key.path(), true);
  REQUIRE(ctx.Create(kQueue, 2, kHint) == 0);
  ShmConsumer consumer(ctx);
  ShmProducer producer(ctx);

  int before = 1;
  REQUIRE(producer.Produce(before) == 0);                 // seq 1 committed
  RunRogue(key.path(), /*stamp=*/true, /*expected_claim=*/2);
  CHECK(Claimed(ctx) == 2);
  CHECK(Committed(ctx) == 1);             // wedged: slot 2 never committed

  // This Produce claims seq 3 and must wait for seq 2. After LIVENESS_PROBE_NS
  // (100 ms) it probes the stamped pid, finds it dead, skips slot 2, commits 3.
  auto t0 = std::chrono::steady_clock::now();
  int after = 2;
  REQUIRE(producer.Produce(after) == 0);
  auto elapsed = std::chrono::steady_clock::now() - t0;
  CHECK(elapsed >= std::chrono::milliseconds(90));
  CHECK(elapsed < std::chrono::seconds(5));               // well under the 10 s force-skip

  CHECK(Committed(ctx) == 3);
  CHECK(Claimed(ctx) == 3);

  // The consumer sees 1, then 2 (the skipped slot is swallowed transparently).
  CHECK(As<int>(consumer.Consume()) == 1);
  auto buf = consumer.TryConsume();
  REQUIRE(buf.data != nullptr);
  CHECK(As<int>(buf) == 2);
  CHECK(buf.topic != kSkipTopicSentinel);
  CHECK(consumer.TryConsume().data == nullptr);
  CHECK(ctx.Destroy() == 0);
}

TEST_CASE("Tier-1: topic-filtered TryConsume also skips the abandoned slot", "[recovery][tier1][topics]")
{
  ScopedShmKeyFile key;
  ShmContext ctx(key.path(), true);
  REQUIRE(ctx.Create(kQueue, 2, kHint) == 0);
  ShmConsumer consumer(ctx);
  ShmProducer producer(ctx);

  RunRogue(key.path(), true, 1);
  int v = 5;
  REQUIRE(producer.ProduceWithTopic(v, 4) == 0);          // seq 2, after skipping 1

  SubscribedTopics subs;
  subs.set();                                              // all 64 topics; sentinel is 0xFFFF
  auto buf = consumer.TryConsume(subs);
  REQUIRE(buf.data != nullptr);
  CHECK(buf.topic == 4);
  CHECK(As<int>(buf) == 5);
  CHECK(consumer.GetCursor().first == 3);
  CHECK(ctx.Destroy() == 0);
}

TEST_CASE("Tier-1: a consumer blocked in another process is released by recovery", "[recovery][tier1][multiprocess]")
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
    auto buf = consumer.Consume();                        // blocks on seq 1 -> skipped -> seq 2
    const Packet& p = As<Packet>(buf);
    return (p.Valid() && p.index == 77) ? 0 : 6;
  });
  REQUIRE(ready.WaitReady());

  RunRogue(key.path(), true, 1);                           // slot 1 abandoned
  ShmProducer producer(ctx);
  Packet p;
  p.Stamp(0, 77);
  REQUIRE(producer.Produce(p) == 0);                       // recovers 1, commits 2
  CHECK(WaitChild(consumer_pid, std::chrono::seconds(10)) == 0);
  CHECK(ctx.Destroy() == 0);
}

TEST_CASE("Tier-1: several dead producers ahead are recovered one by one", "[recovery][tier1]")
{
  ScopedShmKeyFile key;
  ShmContext ctx(key.path(), true);
  REQUIRE(ctx.Create(kQueue, 2, kHint) == 0);
  ShmConsumer consumer(ctx);
  ShmProducer producer(ctx);

  for (int64_t s = 1; s <= 3; ++s) RunRogue(key.path(), true, s);
  CHECK(Claimed(ctx) == 3);
  CHECK(Committed(ctx) == 0);

  auto t0 = std::chrono::steady_clock::now();
  int v = 9;
  REQUIRE(producer.Produce(v) == 0);                       // seq 4
  CHECK(std::chrono::steady_clock::now() - t0 < std::chrono::seconds(5));
  CHECK(Committed(ctx) == 4);

  auto buf = consumer.TryConsume();
  REQUIRE(buf.data != nullptr);
  CHECK(As<int>(buf) == 9);
  CHECK(consumer.GetCursor().first == 5);
  CHECK(ctx.Destroy() == 0);
}

TEST_CASE("Tier-2: unstamped dead producer is force-skipped after the timeout", "[recovery][tier2][slow]")
{
  ScopedShmKeyFile key;
  ShmContext ctx(key.path(), true);
  REQUIRE(ctx.Create(kQueue, 2, kHint) == 0);
  ShmConsumer consumer(ctx);
  ShmProducer producer(ctx);

  RunRogue(key.path(), /*stamp=*/false, 1);                // no pid stamp -> Tier-1 cannot act
  CHECK(Claimed(ctx) == 1);
  CHECK(Committed(ctx) == 0);

  auto t0 = std::chrono::steady_clock::now();
  int v = 11;
  REQUIRE(producer.Produce(v) == 0);
  auto elapsed = std::chrono::steady_clock::now() - t0;
  CHECK(elapsed >= std::chrono::seconds(9));               // FORCE_SKIP_NS = 10 s
  CHECK(elapsed < std::chrono::seconds(40));

  CHECK(Committed(ctx) == 2);
  auto buf = consumer.TryConsume();
  REQUIRE(buf.data != nullptr);
  CHECK(As<int>(buf) == 11);
  CHECK(ctx.Destroy() == 0);
}
