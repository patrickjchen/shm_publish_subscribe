// Attach and consume until <expected> messages arrive or <timeout_s> passes.
// Prints READY once the cursor is registered (so a script can start producers).
// Exit 0 iff every expected message arrived, verified, and in per-producer order.
// usage: shm_consumer <key-file> <expected> [timeout_s=30] [topic=all]
#include "demo_message.h"
#include "shmctx/shmctx.h"
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <map>

int main(int argc, char** argv)
{
  if (argc < 3) { std::fprintf(stderr, "usage: %s <key-file> <expected> [timeout_s] [topic]\n", argv[0]); return 1; }
  uint32_t expected = uint32_t(std::atoi(argv[2]));
  int timeout_s = argc > 3 ? std::atoi(argv[3]) : 30;
  int topic = argc > 4 ? std::atoi(argv[4]) : -1;

  mcp::core::ShmContext ctx(argv[1], true);
  if (ctx.Attach() != 0) { std::fprintf(stderr, "Attach failed\n"); return 1; }
  mcp::core::ShmConsumer consumer(ctx);
  mcp::core::SubscribedTopics subs;
  if (topic < 0) subs.set(); else subs.set(size_t(topic));
  std::printf("READY\n"); std::fflush(stdout);

  std::map<uint32_t, uint32_t> next;
  uint32_t got = 0, bad = 0, misordered = 0;
  int64_t max_lat = 0;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
  while (got < expected && std::chrono::steady_clock::now() < deadline) {
    auto buf = consumer.TryConsume(subs);
    if (buf.data == nullptr) continue;
    const auto& m = *reinterpret_cast<const DemoMessage*>(buf.data);
    if (!m.Valid()) ++bad;
    if (m.index != next[m.producer_id]++) ++misordered;
    auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                 std::chrono::system_clock::now().time_since_epoch()).count();
    if (now - m.publish_ns > max_lat) max_lat = now - m.publish_ns;
    ++got;
  }
  std::printf("consumer: received=%u expected=%u bad=%u misordered=%u producers=%zu max_latency=%ld ns\n",
              got, expected, bad, misordered, next.size(), max_lat);
  return (got == expected && bad == 0 && misordered == 0) ? 0 : 2;
}
