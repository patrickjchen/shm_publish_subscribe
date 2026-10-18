#pragma once
// Wire message shared by the example binaries. The first two fields are
// stamped by PacketProducer (sequence number, then publish timestamp in ns).
#include <cstddef>
#include <cstdint>
#include <cstdio>

struct DemoMessage
{
  uint64_t seq = 0;
  int64_t publish_ns = 0;
  uint32_t producer_id = 0;
  uint32_t index = 0;
  double price = 0.0;
  double qty = 0.0;
  uint64_t checksum = 0;

  static constexpr size_t kSeqOffset = 0;
  static constexpr size_t kTsOffset = sizeof(uint64_t);

  uint64_t Compute() const
  {
    uint64_t h = 1469598103934665603ULL;
    auto mix = [&](uint64_t v) { h ^= v; h *= 1099511628211ULL; };
    mix(producer_id); mix(index);
    uint64_t p, q; __builtin_memcpy(&p, &price, 8); __builtin_memcpy(&q, &qty, 8);
    mix(p); mix(q);
    return h;
  }
  void Seal() { checksum = Compute(); }
  bool Valid() const { return checksum == Compute(); }
};

inline void TouchKeyFile(const char* path)
{
  // ftok() needs an existing file; create it if missing.
  if (FILE* f = std::fopen(path, "a")) std::fclose(f);
}
