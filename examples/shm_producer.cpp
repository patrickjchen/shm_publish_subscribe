// Attach to an existing queue and publish <count> DemoMessages.
// usage: shm_producer <key-file> <producer-id> <count> [topic=1] [pause_us=0]
#include "demo_message.h"
#include "shmctx/shmctx.h"
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

int main(int argc, char** argv)
{
  if (argc < 4) { std::fprintf(stderr, "usage: %s <key-file> <producer-id> <count> [topic] [pause_us]\n", argv[0]); return 1; }
  uint32_t id = uint32_t(std::atoi(argv[2]));
  uint32_t count = uint32_t(std::atoi(argv[3]));
  uint16_t topic = argc > 4 ? uint16_t(std::atoi(argv[4])) : 1;
  int pause_us = argc > 5 ? std::atoi(argv[5]) : 0;

  mcp::core::ShmContext ctx(argv[1], true);
  if (ctx.Attach() != 0) { std::fprintf(stderr, "Attach failed (is the segment created?)\n"); return 1; }
  mcp::core::PacketProducer producer(ctx, DemoMessage::kSeqOffset, DemoMessage::kTsOffset);

  auto t0 = std::chrono::steady_clock::now();
  for (uint32_t i = 0; i < count; ++i) {
    DemoMessage m;
    m.producer_id = id; m.index = i; m.price = 100.0 + (i % 1000) * 0.01; m.qty = 1.0 + id;
    m.Seal();
    producer.ProduceWithTopic(m, topic);
    if (pause_us > 0) std::this_thread::sleep_for(std::chrono::microseconds(pause_us));
  }
  auto us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0).count();
  std::printf("producer %u: sent %u on topic %u in %ld us; queue seq now %ld\n",
              id, count, topic, us, ctx.GetCursor());
  return 0;
}
