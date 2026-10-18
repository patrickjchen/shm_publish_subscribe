// 1 GiB queues: geometry, offsets past the ring size, huge bursts without
// overrun, and multi-process traffic over a 1 GiB SysV segment. Serialized via
// ctest RESOURCE_LOCK because each test commits ~1.1 GiB.
#include "../integration/process_utils.h"
#include "shm_test_support.h"
#include "shmctx/task.h"

#include <catch2/catch_test_macros.hpp>
#include <latch>
#include <thread>

using namespace mcp::core;
using namespace shmctx_test;

namespace {
constexpr int64_t kNeed = kBigQueueBytes + (512LL << 20);   // queue + headroom for the test process
}

TEST_CASE("1 GiB private queue: geometry and wrap-around with 1 MiB messages", "[large][geometry]")
{
  SHMCTX_REQUIRE_MEMORY(kNeed);
  ShmContext ctx("x", false);
  auto t0 = std::chrono::steady_clock::now();
  REQUIRE(ctx.Create(kBigQueueBytes, 4, kBigSizeHint) == 0);
  auto create_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
  WARN("1 GiB private Create took " << create_ms << " ms");

  CHECK(ctx.GetStorageSize() == kBigQueueBytes);
  CHECK(ctx.GetLedgerSize() == kBigQueueBytes / kBigSizeHint);     // 2,097,152 slots
  CHECK(ctx.GetConsumers() == 4);

  // 1025 x 1 MiB = 1 GiB + 1 MiB: the last message crosses the 1 GiB boundary
  // and must land at the start of storage, intact.
  constexpr int64_t kMsg = 1 << 20;
  std::vector<char> frame(kMsg);
  ShmProducer producer(ctx);
  ShmConsumer consumer(ctx);
  const int kCount = 1025;
  for (int i = 0; i < kCount; ++i) {
    std::memset(frame.data(), char('A' + i % 26), kMsg);
    *reinterpret_cast<int*>(frame.data()) = i;
    *reinterpret_cast<int*>(frame.data() + kMsg - sizeof(int)) = ~i;
    REQUIRE(producer.Produce(frame.data(), kMsg) == 0);
    auto buf = consumer.Consume();
    REQUIRE(buf.size == kMsg);
    CHECK(*reinterpret_cast<const int*>(buf.data) == i);
    CHECK(*reinterpret_cast<const int*>(buf.data + kMsg - sizeof(int)) == ~i);
    CHECK(buf.data[kMsg / 2] == char('A' + i % 26));
    CHECK(buf.data - ctx.GetStorage() + buf.size <= ctx.GetStorageSize());
  }
  // Offsets beyond the ring size are handled (int64 offsets throughout).
  auto last = consumer.GetCursor();
  CHECK(last.second == kCount);
}

TEST_CASE("1 GiB queue absorbs a 1M-message burst before the consumer starts: no overrun", "[large][task]")
{
  SHMCTX_REQUIRE_MEMORY(kNeed);
  ShmContext ctx("x", false);
  REQUIRE(ctx.Create(kBigQueueBytes, 4, kBigSizeHint) == 0);

  using namespace shmctx;
  TaskSpec src; src.name = "src"; src.type = TaskType::Producer; src.queue = "q"; src.id = 1; src.topic = 1;
  src.count = 1'000'000; src.size = 128;
  TaskSpec sink; sink.name = "sink"; sink.type = TaskType::Consumer; sink.queue = "q"; sink.subscribe = ParseTopicMask("1");
  sink.ends = 1; sink.timeout_s = 120;

  // The consumer registers its cursor, then deliberately does not read until
  // the whole burst (1M x 128 B = 128 MB, 1M ledger slots) has been published.
  std::latch registered(1), burst_done(1);
  TaskStats cs;
  std::thread c([&] {
    cs = RunConsumer(ctx, sink, [&] { registered.count_down(); burst_done.wait(); });
  });
  registered.wait();
  TaskStats ps = RunProducer(ctx, src);
  burst_done.count_down();
  c.join();

  CHECK(ps.produced == 1'000'000);
  CHECK(cs.ok);
  CHECK(cs.received == 1'000'000);
  CHECK(cs.bad == 0);
  CHECK(cs.misordered == 0);
  CHECK(cs.max_lag >= 1'000'000);           // it really was a full burst behind
  CHECK(cs.overruns == 0);                  // ...and the 2M-slot ledger held all of it
  CHECK(ctx.GetLedgerSize() > cs.max_lag);
}

TEST_CASE("1 GiB SysV segment: 4 producer processes, parent drains 1M messages", "[large][multiprocess]")
{
  SHMCTX_REQUIRE_MEMORY(kNeed);
  ScopedShmKeyFile key;
  ShmContext ctx(key.path(), true);
  auto t0 = std::chrono::steady_clock::now();
  REQUIRE(ctx.Create(kBigQueueBytes, 8, kBigSizeHint) == 0);
  WARN("1 GiB SysV Create took "
       << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count() << " ms");
  ShmConsumer consumer(ctx);

  constexpr int kProcs = 4;
  constexpr uint32_t kPer = 250'000;
  constexpr int64_t kMsgBytes = 256;
  std::vector<pid_t> kids;
  for (int p = 0; p < kProcs; ++p) {
    kids.push_back(ForkChild([&, p] {
      ShmContext c(key.path(), true);
      if (c.Attach() != 0) return 2;
      PacketProducer prod(c, Packet::kSeqOffset, Packet::kTsOffset);
      std::vector<char> frame(kMsgBytes, char(p));
      auto* pk = reinterpret_cast<Packet*>(frame.data());
      for (uint32_t i = 0; i < kPer; ++i) {
        pk->Stamp(uint32_t(p), i);
        prod.Produce(frame.data(), kMsgBytes);
      }
      return 0;
    }));
  }

  std::vector<uint32_t> next(kProcs, 0);
  uint64_t got = 0, bad = 0, misordered = 0;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
  while (got < uint64_t(kProcs) * kPer && std::chrono::steady_clock::now() < deadline) {
    auto buf = consumer.TryConsume();
    if (buf.data == nullptr) { std::this_thread::yield(); continue; }
    if (buf.size != kMsgBytes) { ++bad; ++got; continue; }
    const Packet& p = As<Packet>(buf);
    if (!p.Valid() || p.producer >= uint32_t(kProcs) || buf.data[kMsgBytes - 1] != char(p.producer)) ++bad;
    else if (p.index != next[p.producer]++) ++misordered;
    ++got;
  }
  for (pid_t k : kids) CHECK(WaitChild(k, std::chrono::seconds(120)) == 0);
  CHECK(got == uint64_t(kProcs) * kPer);
  CHECK(bad == 0);
  CHECK(misordered == 0);
  CHECK(ctx.GetCursor() == int64_t(kProcs) * kPer);
  CHECK(ctx.Destroy() == 0);
}
