// In-process task runtime: producer / consumer / relay over private contexts.
#include "shm_test_support.h"
#include "shmctx/task.h"

#include <catch2/catch_test_macros.hpp>
#include <latch>
#include <thread>

using namespace shmctx;
namespace mc = mcp::core;

namespace {
struct Q
{
  mc::ShmContext ctx{ "x", false };
  Q() { REQUIRE(ctx.Create(1 << 22, 4, 64) == 0); }
};

TaskSpec Producer(const char* name, uint32_t id, uint16_t topic, uint32_t count, uint16_t strat = 0, uint32_t size = sizeof(TaskMessage))
{
  TaskSpec t; t.name = name; t.type = TaskType::Producer; t.queue = "q"; t.id = id; t.topic = topic; t.count = count;
  t.strategy_id = strat; t.size = size;
  return t;
}
TaskSpec Consumer(const char* name, const char* mask, uint32_t ends, int timeout_s = 10)
{
  TaskSpec t; t.name = name; t.type = TaskType::Consumer; t.queue = "q"; t.subscribe = ParseTopicMask(mask); t.ends = ends; t.timeout_s = timeout_s;
  return t;
}
TaskSpec Relay(const char* name, uint32_t id, const char* mask, uint16_t out_topic, uint32_t ends, int timeout_s = 10)
{
  TaskSpec t; t.name = name; t.type = TaskType::Relay; t.in = "a"; t.out = "b"; t.id = id; t.subscribe = ParseTopicMask(mask);
  t.out_topic = out_topic; t.ends = ends; t.timeout_s = timeout_s;
  return t;
}
} // namespace

TEST_CASE("TaskMessage checksum covers routing fields but not hop-local stamps", "[task][message]")
{
  TaskMessage m;
  m.origin = 3; m.index = 9; m.size = sizeof(TaskMessage); m.origin_ts = 123;
  m.Seal();
  CHECK(m.Valid());
  m.seq = 77; m.ts = 88;             // re-stamped by every hop
  CHECK(m.Valid());
  m.hops = 1;
  CHECK_FALSE(m.Valid());            // relay must re-seal after transforming
  m.Seal();
  CHECK(m.Valid());
}

TEST_CASE("Producer then consumer: End marker terminates, counts match", "[task][runtime]")
{
  Q q;
  std::latch ready(1);
  TaskStats cs;
  std::thread c([&] { cs = RunConsumer(q.ctx, Consumer("sink", "1", 1), [&] { ready.count_down(); }); });
  ready.wait();
  auto ps = RunProducer(q.ctx, Producer("src", 1, 1, 1000, 0, 200));
  c.join();

  CHECK(ps.ok);
  CHECK(ps.produced == 1000);
  CHECK(cs.ok);
  CHECK(cs.received == 1000);
  CHECK(cs.ends_seen == 1);
  CHECK(cs.bad == 0);
  CHECK(cs.misordered == 0);
  CHECK(cs.per_origin.at(1) == 1000);
  CHECK(cs.max_hops == 0);
  CHECK(cs.max_latency_ns > 0);
  CHECK_FALSE(cs.timed_out);
}

TEST_CASE("Consumer mask filters topics; End markers still counted from subscribed topics only", "[task][runtime][mask]")
{
  Q q;
  std::latch ready(1);
  TaskStats cs;
  std::thread c([&] { cs = RunConsumer(q.ctx, Consumer("sink", "0b0110", 2), [&] { ready.count_down(); }); });
  ready.wait();
  RunProducer(q.ctx, Producer("p3", 3, 3, 500));   // topic 3: invisible to the sink
  RunProducer(q.ctx, Producer("p1", 1, 1, 400));
  RunProducer(q.ctx, Producer("p2", 2, 2, 300));
  c.join();
  CHECK(cs.ok);
  CHECK(cs.received == 700);
  CHECK(cs.per_origin.count(3) == 0);
  CHECK(cs.per_origin.at(1) == 400);
  CHECK(cs.per_origin.at(2) == 300);
  CHECK(cs.ends_seen == 2);
}

TEST_CASE("Consumer strategy filter: data filtered, broadcast End markers pass", "[task][runtime][strategy]")
{
  Q q;
  auto spec = Consumer("only7", "4", 2);
  spec.strategy_ids = { 7 };
  spec.expect = 0;
  std::latch ready(1);
  TaskStats cs;
  std::thread c([&] { cs = RunConsumer(q.ctx, spec, [&] { ready.count_down(); }); });
  ready.wait();
  RunProducer(q.ctx, Producer("s7", 7, 4, 100, 7));
  RunProducer(q.ctx, Producer("s9", 9, 4, 100, 9));
  c.join();
  CHECK(cs.ok);
  CHECK(cs.received == 100);
  CHECK(cs.per_origin.count(9) == 0);
  CHECK(cs.ends_seen == 2);
}

TEST_CASE("Consumer with expect stops early and flags a count mismatch", "[task][runtime]")
{
  Q q;
  auto spec = Consumer("sink", "1", 1);
  spec.expect = 250;
  std::latch ready(1);
  TaskStats cs;
  std::thread c([&] { cs = RunConsumer(q.ctx, spec, [&] { ready.count_down(); }); });
  ready.wait();
  RunProducer(q.ctx, Producer("src", 1, 1, 1000));
  c.join();
  CHECK(cs.ok);
  CHECK(cs.received == 250);

  // Fewer messages than expected -> timeout -> not ok.
  auto late = Consumer("late", "2", 1, /*timeout_s=*/1);
  late.expect = 10;
  auto ls = RunConsumer(q.ctx, late);
  CHECK(ls.timed_out);
  CHECK_FALSE(ls.ok);
}

TEST_CASE("Relay: consumes from A with a mask, republishes on B with hops++ and relay id", "[task][runtime][relay]")
{
  Q a, b;
  std::latch ready(2);
  TaskStats rs, cs;
  std::thread relay([&] { rs = RunRelay(a.ctx, b.ctx, Relay("relay", 10, "0b0110", 5, 2), [&] { ready.count_down(); }); });
  std::thread sink([&] { cs = RunConsumer(b.ctx, Consumer("sink", "5", 2), [&] { ready.count_down(); }); });
  ready.wait();
  auto p3 = RunProducer(a.ctx, Producer("noise", 3, 3, 200));
  auto p1 = RunProducer(a.ctx, Producer("p1", 1, 1, 3000, 0, 120));
  auto p2 = RunProducer(a.ctx, Producer("p2", 2, 2, 2000));
  relay.join(); sink.join();

  CHECK(rs.ok);
  CHECK(rs.received == 5000);
  CHECK(rs.produced == 5000);
  CHECK(rs.ends_seen == 2);
  CHECK(cs.ok);
  CHECK(cs.received == 5000);
  CHECK(cs.per_origin.at(1) == 3000);
  CHECK(cs.per_origin.at(2) == 2000);
  CHECK(cs.per_origin.count(3) == 0);
  CHECK(cs.max_hops == 1);
  CHECK(cs.ends_seen == 2);
  CHECK(cs.misordered == 0);
  CHECK(cs.bad == 0);
}

TEST_CASE("Relay chain on a single queue: topics 1 -> 2 -> 3, hops == 2 at the sink", "[task][runtime][relay]")
{
  Q bus;
  std::latch ready(3);
  TaskStats ra, rb, cs;
  auto hop_a = Relay("hop_a", 11, "1", 2, 1); hop_a.in = hop_a.out = "bus";
  auto hop_b = Relay("hop_b", 12, "2", 3, 1); hop_b.in = hop_b.out = "bus";
  std::thread ta([&] { ra = RunRelay(bus.ctx, bus.ctx, hop_a, [&] { ready.count_down(); }); });
  std::thread tb([&] { rb = RunRelay(bus.ctx, bus.ctx, hop_b, [&] { ready.count_down(); }); });
  std::thread tc([&] { cs = RunConsumer(bus.ctx, Consumer("sink", "3", 1), [&] { ready.count_down(); }); });
  ready.wait();
  RunProducer(bus.ctx, Producer("src", 1, 1, 10000));
  ta.join(); tb.join(); tc.join();

  CHECK(ra.ok); CHECK(rb.ok); CHECK(cs.ok);
  CHECK(ra.received == 10000);
  CHECK(rb.received == 10000);
  CHECK(cs.received == 10000);
  CHECK(cs.max_hops == 2);
  CHECK(cs.misordered == 0);
  // Every hop republished its stream, so the bus carries 3 x (10000 + End).
  CHECK(bus.ctx.GetCursor() == 3 * 10001);
}

TEST_CASE("Relay times out when upstream never finishes", "[task][runtime][relay]")
{
  Q a, b;
  auto spec = Relay("relay", 1, "1", 2, /*ends=*/1, /*timeout_s=*/1);
  auto rs = RunRelay(a.ctx, b.ctx, spec);
  CHECK(rs.timed_out);
  CHECK_FALSE(rs.ok);
  CHECK(rs.received == 0);
}

TEST_CASE("RunTask dispatches and resolves queue references", "[task][runtime]")
{
  Q a, b;
  std::map<std::string, mc::ShmContext*> queues{ { "a", &a.ctx }, { "b", &b.ctx } };
  int resolves = 0;
  auto resolve = [&](const std::string& ref) -> mc::ShmContext& { ++resolves; return *queues.at(ref); };

  auto relay_spec = Relay("relay", 5, "all", 7, 1);        // in=a, out=b
  auto sink_spec = Consumer("sink", "7", 1); sink_spec.queue = "b";
  auto src_spec = Producer("src", 1, 0, 100); src_spec.queue = "a";

  std::latch ready(2);
  TaskStats rs, cs;
  std::thread r([&] { rs = RunTask(relay_spec, resolve, [&] { ready.count_down(); }); });
  std::thread c([&] { cs = RunTask(sink_spec, resolve, [&] { ready.count_down(); }); });
  ready.wait();
  auto ps = RunTask(src_spec, resolve);
  r.join(); c.join();
  CHECK(ps.ok); CHECK(rs.ok); CHECK(cs.ok);
  CHECK(cs.received == 100);
  CHECK(resolves == 4);                                     // relay: 2, sink: 1, src: 1

  // Same-queue relay resolves its reference once.
  auto loop = Relay("loop", 6, "1", 2, 1, 1); loop.in = loop.out = "a";
  resolves = 0;
  RunTask(loop, resolve);
  CHECK(resolves == 1);

  // Self-loop whose mask includes its own out_topic is rejected before running.
  auto bad = Relay("bad", 1, "all", 0, 1); bad.in = bad.out = "a";
  CHECK_THROWS_AS(RunTask(bad, resolve), std::invalid_argument);
}

TEST_CASE("Consumer reports lag and overrun when the ledger is lapped", "[task][runtime][overrun]")
{
  // 4 KiB storage / size_hint 64 -> 64 ledger slots. Publish 1000 messages
  // before the consumer reads anything: it is lapped many times over.
  mc::ShmContext small("x", false);
  REQUIRE(small.Create(4096, 1, 64) == 0);
  REQUIRE(small.GetLedgerSize() == 64);
  auto spec = Consumer("sink", "1", 1, /*timeout_s=*/1);
  spec.check_order = false;
  mc::ShmConsumer probe(small);   // only to prove the head moved
  TaskStats cs;
  std::latch ready(1);
  std::thread c([&] {
    cs = RunConsumer(small, spec, [&] { ready.count_down(); std::this_thread::sleep_for(std::chrono::milliseconds(50)); });
  });
  ready.wait();
  RunProducer(small, Producer("src", 1, 1, 1000));
  c.join();
  CHECK(probe.GetCursor().second == 1001);
  CHECK(cs.max_lag >= 64);
  CHECK(cs.overruns > 0);
  CHECK(cs.ToString().find("OVERRUN") != std::string::npos);

  // Healthy case: ledger larger than the burst -> no overrun flagged.
  Q big;
  TaskStats hs;
  std::latch ready2(1);
  std::thread h([&] { hs = RunConsumer(big.ctx, Consumer("sink", "1", 1), [&] { ready2.count_down(); }); });
  ready2.wait();
  RunProducer(big.ctx, Producer("src", 1, 1, 1000));
  h.join();
  CHECK(hs.ok);
  CHECK(hs.overruns == 0);
  CHECK(hs.max_lag <= 1001);
}

TEST_CASE("TaskStats::ToString is one line with the key counters", "[task][stats]")
{
  TaskStats s; s.name = "x"; s.type = TaskType::Relay; s.ok = true; s.produced = 5; s.received = 6; s.timed_out = true;
  auto str = s.ToString();
  CHECK(str.find("x[relay]") == 0);
  CHECK(str.find("produced=5") != std::string::npos);
  CHECK(str.find("received=6") != std::string::npos);
  CHECK(str.find("TIMEOUT") != std::string::npos);
  CHECK(str.find('\n') == std::string::npos);
}
