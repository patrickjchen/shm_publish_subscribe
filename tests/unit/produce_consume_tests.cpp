// Core produce/consume semantics: ordering, wrap-around, TryConsume, batches.
#include "shm_test_support.h"

#include <catch2/catch_test_macros.hpp>
#include <cstring>
#include <numeric>

using namespace mcp::core;
using namespace shmctx_test;

namespace {
struct Ctx
{
  ShmContext ctx{ "x", false };
  explicit Ctx(int64_t queue = 1 << 12, int64_t consumers = 1, int64_t hint = 32)
  {
    REQUIRE(ctx.Create(queue, consumers, hint) == 0);
  }
};
} // namespace

TEST_CASE("Single message round-trip", "[produce][consume]")
{
  Ctx c;
  ShmProducer producer(c.ctx);
  ShmConsumer consumer(c.ctx);   // constructed before producing -> sees everything after

  Packet p;
  p.Stamp(1, 7);
  REQUIRE(producer.Produce(p) == 0);
  CHECK(Committed(c.ctx) == 1);
  CHECK(Claimed(c.ctx) == 1);

  auto buf = consumer.Consume();
  REQUIRE(buf.size == int64_t(sizeof(Packet)));
  CHECK(buf.topic == 0);
  CHECK(buf.strategy_id == 0);
  CHECK(As<Packet>(buf).Valid());
  CHECK(As<Packet>(buf).index == 7);
}

TEST_CASE("Raw byte round-trip preserves content and size", "[produce][consume]")
{
  Ctx c;
  ShmProducer producer(c.ctx);
  ShmConsumer consumer(c.ctx);
  const char msg[] = "hello shared memory";
  REQUIRE(producer.Produce(msg, sizeof(msg)) == 0);
  auto buf = consumer.Consume();
  REQUIRE(buf.size == int64_t(sizeof(msg)));
  CHECK(std::memcmp(buf.data, msg, sizeof(msg)) == 0);
}

TEST_CASE("TryConsume returns an empty wrapper when nothing is available", "[consume][try]")
{
  Ctx c;
  ShmConsumer consumer(c.ctx);
  auto buf = consumer.TryConsume();
  CHECK(buf.data == nullptr);
  CHECK(buf.size == -1);
  // Cursor is not advanced by a miss.
  CHECK(consumer.GetCursor().first == 1);
}

TEST_CASE("Messages are delivered in FIFO order", "[produce][consume]")
{
  Ctx c;
  ShmProducer producer(c.ctx);
  ShmConsumer consumer(c.ctx);
  for (uint32_t i = 0; i < 50; ++i) {
    Packet p;
    p.Stamp(0, i);
    REQUIRE(producer.Produce(p) == 0);
  }
  for (uint32_t i = 0; i < 50; ++i) {
    auto buf = consumer.TryConsume();
    REQUIRE(buf.data != nullptr);
    CHECK(As<Packet>(buf).index == i);
    CHECK(As<Packet>(buf).Valid());
  }
  CHECK(consumer.TryConsume().data == nullptr);
}

TEST_CASE("Variable-size messages are each delivered with their own size", "[produce][consume]")
{
  Ctx c;
  ShmProducer producer(c.ctx);
  ShmConsumer consumer(c.ctx);
  std::vector<std::string> msgs = { "a", "bb", "ccc", std::string(100, 'd'), "e" };
  for (auto& m : msgs) REQUIRE(producer.Produce(m.data(), int64_t(m.size())) == 0);
  for (auto& m : msgs) {
    auto buf = consumer.Consume();
    REQUIRE(buf.size == int64_t(m.size()));
    CHECK(std::string(buf.data, buf.size) == m);
  }
}

TEST_CASE("Consumer created late only sees messages after its creation", "[consume][cursor]")
{
  Ctx c;
  ShmProducer producer(c.ctx);
  int a = 1, b = 2;
  REQUIRE(producer.Produce(a) == 0);
  ShmConsumer consumer(c.ctx);   // cursor = current + 1
  REQUIRE(producer.Produce(b) == 0);
  auto buf = consumer.TryConsume();
  REQUIRE(buf.data != nullptr);
  CHECK(As<int>(buf) == 2);
  CHECK(consumer.TryConsume().data == nullptr);
}

TEST_CASE("Storage wraps around without corrupting messages", "[produce][wrap]")
{
  // 4 KiB storage, 128 B messages -> wrap every 32 messages; ledger = 4096/128 = 32 slots.
  Ctx c(4096, 1, 128);
  ShmProducer producer(c.ctx);
  ShmConsumer consumer(c.ctx);

  struct Big { uint32_t idx; char pad[124]; };
  static_assert(sizeof(Big) == 128);

  // Lockstep so we never overwrite unread slots.
  for (uint32_t i = 0; i < 500; ++i) {
    Big b{};
    b.idx = i;
    std::memset(b.pad, char('a' + i % 26), sizeof(b.pad));
    REQUIRE(producer.Produce(b) == 0);
    auto buf = consumer.Consume();
    REQUIRE(buf.size == 128);
    const Big& got = As<Big>(buf);
    CHECK(got.idx == i);
    CHECK(got.pad[0] == char('a' + i % 26));
    CHECK(got.pad[123] == char('a' + i % 26));
    // Payload must never straddle the end of storage.
    auto start = buf.data - c.ctx.GetStorage();
    CHECK(start + buf.size <= c.ctx.GetStorageSize());
  }
}

TEST_CASE("Message that does not fit at the tail is placed at the start of storage", "[produce][wrap]")
{
  // 256 B storage, size_hint 16 -> ledger 16.
  Ctx c(256, 1, 16);
  ShmProducer producer(c.ctx);
  ShmConsumer consumer(c.ctx);

  std::vector<char> a(200, 'A'), b(100, 'B');
  REQUIRE(producer.Produce(a.data(), 200) == 0);
  REQUIRE(producer.Produce(b.data(), 100) == 0);   // only 55 bytes left at tail -> wraps

  auto ba = consumer.Consume();
  auto bb = consumer.Consume();
  CHECK(ba.data - c.ctx.GetStorage() == 1);        // first offset is 1
  CHECK(bb.data - c.ctx.GetStorage() == 0);        // wrapped to the beginning
  CHECK(std::count(bb.data, bb.data + 100, 'B') == 100);
}

TEST_CASE("Batch produce concatenates buffers into one message", "[produce][batch]")
{
  Ctx c;
  ShmProducer producer(c.ctx);
  ShmConsumer consumer(c.ctx);

  std::string s1 = "first-", s2 = "second-", s3 = "third";
  std::vector<BufferWrapper> batch = {
    { s1.data(), int64_t(s1.size()) },
    { s2.data(), int64_t(s2.size()) },
    { s3.data(), int64_t(s3.size()) },
  };
  REQUIRE(producer.Produce(batch) == 0);
  auto buf = consumer.Consume();
  REQUIRE(buf.size == int64_t(s1.size() + s2.size() + s3.size()));
  CHECK(std::string(buf.data, buf.size) == "first-second-third");
}
