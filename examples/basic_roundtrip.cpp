// In-process demo: one context, one producer thread, two consumer threads.
#include "demo_message.h"
#include "shmctx/shmctx.h"

#include <cstdio>
#include <thread>

using namespace mcp::core;

int main()
{
  ShmContext ctx("in-process", /*shared_memory=*/false);
  if (ctx.Create(/*queue_size=*/1 << 20, /*num_consumers=*/2, /*size_hint=*/64) != 0) return 1;
  std::printf("storage=%ld ledger=%ld consumers=%ld\n", ctx.GetStorageSize(), ctx.GetLedgerSize(), ctx.GetConsumers());

  constexpr uint32_t kMessages = 100000;
  ShmConsumer c1(ctx), c2(ctx);   // register cursors before producing

  auto consume = [&](ShmConsumer& c, const char* name) {
    uint32_t got = 0, bad = 0;
    int64_t max_lat = 0;
    while (got < kMessages) {
      auto buf = c.Consume();
      const auto& m = *reinterpret_cast<const DemoMessage*>(buf.data);
      if (!m.Valid() || m.index != got) ++bad;
      auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::system_clock::now().time_since_epoch()).count();
      max_lat = std::max(max_lat, now - m.publish_ns);
      ++got;
    }
    std::printf("%s: received=%u bad=%u max_latency=%ld ns\n", name, got, bad, max_lat);
  };
  std::thread t1(consume, std::ref(c1), "consumer-1");
  std::thread t2(consume, std::ref(c2), "consumer-2");

  PacketProducer producer(ctx, DemoMessage::kSeqOffset, DemoMessage::kTsOffset);
  auto t0 = std::chrono::steady_clock::now();
  for (uint32_t i = 0; i < kMessages; ++i) {
    DemoMessage m;
    m.producer_id = 1; m.index = i; m.price = 100.0 + i * 0.01; m.qty = 1.0;
    m.Seal();
    producer.ProduceWithTopic(m, 1);
  }
  auto us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0).count();
  std::printf("produced %u messages in %ld us (%.1f ns/msg)\n", kMessages, us, us * 1000.0 / kMessages);

  t1.join(); t2.join();
  std::printf("final sequence=%ld\n", ctx.GetCursor());
  return 0;
}
