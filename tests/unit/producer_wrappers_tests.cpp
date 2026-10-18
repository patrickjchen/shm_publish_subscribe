// PacketProducer (seq + timestamp stamping) and BatchProducer.
#include "shm_test_support.h"

#include <catch2/catch_test_macros.hpp>
#include <cstring>

using namespace mcp::core;
using namespace shmctx_test;

namespace {
struct Ctx
{
  ShmContext ctx{ "x", false };
  Ctx() { REQUIRE(ctx.Create(1 << 14, 1, 32) == 0); }
};
} // namespace

TEST_CASE("PacketProducer stamps the claimed sequence number into the payload", "[packet_producer][seq]")
{
  Ctx c;
  PacketProducer producer(c.ctx, Packet::kSeqOffset, Packet::kTsOffset);
  ShmConsumer consumer(c.ctx);

  for (uint32_t i = 0; i < 10; ++i) {
    Packet p;
    p.Stamp(1, i);
    REQUIRE(producer.Produce(reinterpret_cast<char*>(&p), sizeof(p)) == 0);
  }
  for (uint32_t i = 0; i < 10; ++i) {
    auto buf = consumer.Consume();
    const Packet& got = As<Packet>(buf);
    CHECK(got.seq == i + 1);          // sequence numbers start at 1
    CHECK(got.index == i);
    CHECK(got.Valid());
  }
}

TEST_CASE("PacketProducer stamps a wall-clock nanosecond timestamp", "[packet_producer][timestamp]")
{
  Ctx c;
  PacketProducer producer(c.ctx, Packet::kSeqOffset, Packet::kTsOffset);
  ShmConsumer consumer(c.ctx);

  int64_t before = NowNs();
  Packet p;
  p.Stamp(1, 0);
  REQUIRE(producer.Produce(reinterpret_cast<char*>(&p), sizeof(p)) == 0);
  int64_t after = NowNs();

  auto buf = consumer.Consume();
  const Packet& got = As<Packet>(buf);
  CHECK(got.ts >= before);
  CHECK(got.ts <= after);
}

TEST_CASE("PacketProducer raw-pointer and batch overloads stamp seq from batch[0]", "[packet_producer][batch]")
{
  Ctx c;
  PacketProducer producer(c.ctx, Packet::kSeqOffset, Packet::kTsOffset);
  ShmConsumer consumer(c.ctx);

  Packet head; head.Stamp(2, 100);
  std::string tail = "payload-tail";
  std::vector<BufferWrapper> batch = {
    { reinterpret_cast<char*>(&head), int64_t(sizeof(head)) },
    { tail.data(), int64_t(tail.size()) },
  };
  REQUIRE(producer.Produce(batch) == 0);

  Packet raw; raw.Stamp(2, 101);
  REQUIRE(producer.Produce(reinterpret_cast<char*>(&raw), sizeof(raw)) == 0);

  auto b1 = consumer.Consume();
  REQUIRE(b1.size == int64_t(sizeof(Packet) + tail.size()));
  CHECK(As<Packet>(b1).seq == 1);
  CHECK(As<Packet>(b1).index == 100);
  CHECK(std::string(b1.data + sizeof(Packet), tail.size()) == tail);

  auto b2 = consumer.Consume();
  CHECK(As<Packet>(b2).seq == 2);
  CHECK(As<Packet>(b2).index == 101);
  CHECK(As<Packet>(b2).ts > 0);
}

TEST_CASE("PacketProducer::ProduceWithTopic carries topic and stamps seq", "[packet_producer][topics]")
{
  Ctx c;
  PacketProducer producer(c.ctx, Packet::kSeqOffset, Packet::kTsOffset);
  ShmConsumer consumer(c.ctx);
  Packet p; p.Stamp(3, 0);
  REQUIRE(producer.ProduceWithTopic(p, 5) == 0);
  auto buf = consumer.Consume();
  CHECK(buf.topic == 5);
  CHECK(As<Packet>(buf).seq == 1);
}

TEST_CASE("BatchProducer accumulates messages and sends them as one buffer", "[batch_producer]")
{
  Ctx c;
  BatchProducer bp(c.ctx, 4096);
  ShmConsumer consumer(c.ctx);

  CHECK(bp.GetSize() == 0);
  for (uint32_t i = 0; i < 5; ++i) {
    Packet* slot = bp.GetNextMessage<Packet>();
    *slot = Packet{};
    slot->Stamp(9, i);
  }
  CHECK(bp.GetSize() == int64_t(5 * sizeof(Packet)));

  // NOTE: SendBatch() returns the Produce() rc (0 on success) and only clears
  // the buffer when that rc is non-zero -- i.e. with the current library a
  // successful send leaves the batch populated. We pin that behaviour here so
  // a change is noticed, and Clear() explicitly.
  REQUIRE(bp.SendBatch() == 0);
  CHECK(bp.GetSize() == int64_t(5 * sizeof(Packet)));
  bp.Clear();
  CHECK(bp.GetSize() == 0);

  auto buf = consumer.Consume();
  REQUIRE(buf.size == int64_t(5 * sizeof(Packet)));
  for (uint32_t i = 0; i < 5; ++i) {
    const Packet& p = *reinterpret_cast<const Packet*>(buf.data + i * sizeof(Packet));
    CHECK(p.index == i);
    CHECK(p.Valid());
  }
}

TEST_CASE("BatchProducer with non-positive capacity allocates nothing", "[batch_producer]")
{
  Ctx c;
  BatchProducer bp(c.ctx, 0);
  CHECK(bp.GetSize() == 0);
}
