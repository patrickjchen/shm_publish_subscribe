// Unit tests for ShmContext lifecycle and geometry (private mmap mode unless noted).
#include "shm_test_support.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

using namespace mcp::core;
using namespace shmctx_test;

TEST_CASE("Queue and ledger sizes are rounded up to powers of two", "[context][geometry]")
{
  // GetNextPowerOfTwo is private; observe it through Create().
  struct Case { int64_t queue, hint, storage, ledger; };
  for (Case k : { Case{ 1, 1, 1, 1 },
                  Case{ 3, 1, 4, 4 },
                  Case{ 1000, 60, 1024, 16 },
                  Case{ 1024, 64, 1024, 16 },
                  Case{ 1025, 65, 2048, 16 },
                  Case{ 1 << 20, 100, 1 << 20, (1 << 20) / 128 } }) {
    ShmContext ctx("x", false);
    REQUIRE(ctx.Create(k.queue, 1, k.hint) == 0);
    CHECK(ctx.GetStorageSize() == k.storage);
    CHECK(ctx.GetLedgerSize() == k.ledger);
  }
}

TEST_CASE("Create in private (mmap) mode sets geometry", "[context][create]")
{
  ShmContext ctx("unused-in-private-mode", /*shared_memory=*/false);
  REQUIRE(ctx.Create(/*queue_size=*/1000, /*num_consumers=*/4, /*size_hint=*/60) == 0);

  CHECK(ctx.GetStorageSize() == 1024);          // 1000 -> 1024
  CHECK(ctx.GetLedgerSize() == 1024 / 64);      // storage / pow2(size_hint)
  CHECK(ctx.GetConsumers() == 4);
  CHECK(ctx.GetCursor() == 0);
  CHECK(Committed(ctx) == 0);
  CHECK(Claimed(ctx) == 0);
  CHECK(ctx.GetStorage() != nullptr);
}

TEST_CASE("Attach and Destroy are no-ops in private mode", "[context][create]")
{
  ShmContext ctx("x", false);
  CHECK(ctx.Attach() == -1);   // attach only makes sense for SysV shm
  CHECK(ctx.Destroy() == 0);
}

TEST_CASE("Fresh context has no occupied consumers", "[context][admin]")
{
  ShmContext ctx("x", false);
  REQUIRE(ctx.Create(4096, 3, 64) == 0);
  auto stats = Stats(ctx);
  REQUIRE(stats.size() == 3);
  for (auto& s : stats) {
    CHECK_FALSE(s.occupied);
    CHECK(s.sequence_num == 0);
    CHECK(s.consumer_pid == 0);
  }
}

TEST_CASE("Create/Attach/Destroy round-trip in shared-memory mode", "[context][sysv]")
{
  ScopedShmKeyFile key;
  REQUIRE_FALSE(key.SegmentExists());

  {
    ShmContext owner(key.path(), /*shared_memory=*/true);
    REQUIRE(owner.Create(1 << 16, 2, 128) == 0);
    REQUIRE(key.SegmentExists());

    ShmContext other(key.path(), true);
    REQUIRE(other.Attach() == 0);
    CHECK(other.GetStorageSize() == owner.GetStorageSize());
    CHECK(other.GetLedgerSize() == owner.GetLedgerSize());
    CHECK(other.GetConsumers() == 2);

    // A write through one mapping is visible through the other.
    ShmProducer producer(owner);
    ShmConsumer consumer(other);
    int v = 42;
    REQUIRE(producer.Produce(v) == 0);
    auto buf = consumer.TryConsume();
    REQUIRE(buf.data != nullptr);
    CHECK(As<int>(buf) == 42);

    CHECK(owner.Destroy() == 0);
  }
  CHECK_FALSE(key.SegmentExists());
}

TEST_CASE("Attach fails when no segment exists", "[context][sysv]")
{
  ScopedShmKeyFile key;
  ShmContext ctx(key.path(), true);
  CHECK(ctx.Attach() == -1);
}
