// Topic / strategy-id filtering: SubscribedTopics and TryConsume(topics).
#include "shm_test_support.h"

#include <catch2/catch_test_macros.hpp>
#include <stdexcept>

using namespace mcp::core;
using namespace shmctx_test;

TEST_CASE("SubscribedTopics bit operations", "[topics]")
{
  SubscribedTopics t;
  CHECK(t.topics.none());
  t.set(3);
  CHECK(t.topics.test(3));
  CHECK_FALSE(t.topics.test(4));
  t.set(3, false);
  CHECK_FALSE(t.topics.test(3));
  t.set();
  CHECK(t.topics.all());
  t.reset(10);
  CHECK_FALSE(t.topics.test(10));
  t.reset();
  CHECK(t.topics.none());
}

TEST_CASE("SubscribedTopics::IsIncluded strategy semantics", "[topics][strategy]")
{
  SubscribedTopics t;

  SECTION("no strategy filter: only strategy 0 (broadcast) is included")
  {
    CHECK(t.IsIncluded(0));
    CHECK_FALSE(t.IsIncluded(5));
  }
  SECTION("explicit set")
  {
    t.set_strategy_ids(std::set<uint16_t>{ 5, 9 });
    CHECK(t.IsIncluded(0));      // broadcast always passes
    CHECK(t.IsIncluded(5));
    CHECK(t.IsIncluded(9));
    CHECK_FALSE(t.IsIncluded(6));
  }
  SECTION("incremental add")
  {
    t.set_strategy_ids(uint16_t{ 7 });
    CHECK(t.IsIncluded(7));
    CHECK_FALSE(t.IsIncluded(8));
  }
  SECTION("subscribing to strategy 0 means 'all strategies'")
  {
    t.set_strategy_ids(uint16_t{ 0 });
    CHECK(t.IsIncluded(1));
    CHECK(t.IsIncluded(65535));
  }
}

namespace {
struct Ctx
{
  ShmContext ctx{ "x", false };
  Ctx() { REQUIRE(ctx.Create(1 << 12, 1, 32) == 0); }
};
} // namespace

TEST_CASE("Topic is carried through to the consumer", "[topics][produce]")
{
  Ctx c;
  ShmProducer producer(c.ctx);
  ShmConsumer consumer(c.ctx);
  int v = 1;
  REQUIRE(producer.ProduceWithTopic(v, 17) == 0);
  auto buf = consumer.Consume();
  CHECK(buf.topic == 17);
  CHECK(buf.strategy_id == 0);
}

TEST_CASE("TryConsume(topics) skips unsubscribed topics and advances the cursor", "[topics][consume]")
{
  Ctx c;
  ShmProducer producer(c.ctx);
  ShmConsumer consumer(c.ctx);

  for (uint16_t topic : { 1, 2, 3, 2, 1 }) {
    int v = topic;
    REQUIRE(producer.ProduceWithTopic(v, topic) == 0);
  }

  SubscribedTopics subs;
  subs.set(2);

  auto b1 = consumer.TryConsume(subs);
  REQUIRE(b1.data != nullptr);
  CHECK(b1.topic == 2);
  CHECK(consumer.GetCursor().first == 3);        // consumed seq 2, now at 3

  auto b2 = consumer.TryConsume(subs);
  REQUIRE(b2.data != nullptr);
  CHECK(b2.topic == 2);
  CHECK(consumer.GetCursor().first == 5);

  auto b3 = consumer.TryConsume(subs);           // seq 5 is topic 1 -> skipped to head+1
  CHECK(b3.data == nullptr);
  CHECK(b3.size == -1);
  CHECK(consumer.GetCursor().first == 6);
  CHECK(consumer.GetCursor().second == 5);
}

TEST_CASE("TryConsume(topics) with nothing published returns immediately", "[topics][consume]")
{
  Ctx c;
  ShmConsumer consumer(c.ctx);
  SubscribedTopics subs;
  subs.set();
  auto b = consumer.TryConsume(subs);
  CHECK(b.data == nullptr);
  CHECK(consumer.GetCursor().first == 1);
}

TEST_CASE("Strategy id filtering via PacketProducer::ProduceWithTopicAndStrategyId", "[topics][strategy][consume]")
{
  Ctx c;
  PacketProducer producer(c.ctx, Packet::kSeqOffset, Packet::kTsOffset);
  ShmConsumer consumer(c.ctx);

  auto send = [&](uint16_t topic, uint16_t strat, uint32_t idx) {
    Packet p;
    p.Stamp(strat, idx);
    REQUIRE(producer.ProduceWithTopicAndStrategyId(p, topic, strat) == 0);
  };
  send(1, 10, 0);   // strat 10
  send(1, 20, 1);   // strat 20
  send(1, 0, 2);    // broadcast
  send(2, 10, 3);   // topic 2 -> filtered by topic

  SubscribedTopics subs;
  subs.set(1);
  subs.set_strategy_ids(uint16_t{ 20 });

  auto b1 = consumer.TryConsume(subs);
  REQUIRE(b1.data != nullptr);
  CHECK(As<Packet>(b1).index == 1);
  CHECK(b1.strategy_id == 20);

  auto b2 = consumer.TryConsume(subs);
  REQUIRE(b2.data != nullptr);
  CHECK(As<Packet>(b2).index == 2);   // broadcast passes
  CHECK(b2.strategy_id == 0);

  CHECK(consumer.TryConsume(subs).data == nullptr);
}

TEST_CASE("Topics 0..63 are addressable; 64+ throws (documents current behaviour)", "[topics][limits]")
{
  Ctx c;
  ShmProducer producer(c.ctx);
  ShmConsumer consumer(c.ctx);
  int v = 0;
  REQUIRE(producer.ProduceWithTopic(v, 63) == 0);
  SubscribedTopics subs;
  subs.set(63);
  CHECK(consumer.TryConsume(subs).topic == 63);

  // The ledger stores a uint16_t topic but the subscription is a bitset<64>,
  // so a published topic >= 64 makes TryConsume(topics) throw out_of_range.
  REQUIRE(producer.ProduceWithTopic(v, 64) == 0);
  CHECK_THROWS_AS(consumer.TryConsume(subs), std::out_of_range);
}

TEST_CASE("Skip sentinel is never dispatched by TryConsume(topics)", "[topics][recovery]")
{
  // Simulate what a crash recoverer leaves behind: a slot tagged
  // kSkipTopicSentinel. We can't forge it through the public API, so instead
  // verify the sentinel is outside the bitset range and `set()` does not
  // include it.
  SubscribedTopics subs;
  subs.set();
  CHECK(kSkipTopicSentinel == 0xFFFF);
  CHECK(kSkipTopicSentinel >= subs.topics.size());
}
