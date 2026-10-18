#pragma once
// Shared helpers for shmctx unit and integration tests.

#include "shmctx/shmctx.h"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <unistd.h>

namespace shmctx_test {

// ShmContext in shared-memory mode derives its SysV key with ftok(name, 8),
// so `name` must be an existing file. This creates a unique one per test and
// removes both the file and the segment on destruction, even if the test
// forgot to call Destroy().
class ScopedShmKeyFile
{
public:
  ScopedShmKeyFile()
  {
    namespace fs = std::filesystem;
    const char* base = std::getenv("SHMCTX_TEST_TMPDIR");
    fs::path dir = base ? fs::path(base) : fs::temp_directory_path();
    std::string tmpl = (dir / "shmctx_test_XXXXXX").string();
    int fd = ::mkstemp(tmpl.data());
    if (fd < 0) { std::perror("mkstemp"); std::abort(); }
    ::close(fd);
    path_ = tmpl;
  }

  ~ScopedShmKeyFile()
  {
    // Best-effort removal of a segment that may still exist.
    key_t key = ::ftok(path_.c_str(), 8);
    if (key != -1) {
      int id = ::shmget(key, 0, 0);
      if (id >= 0) ::shmctl(id, IPC_RMID, nullptr);
    }
    ::unlink(path_.c_str());
  }

  ScopedShmKeyFile(const ScopedShmKeyFile&) = delete;
  ScopedShmKeyFile& operator=(const ScopedShmKeyFile&) = delete;

  const std::string& path() const { return path_; }

  // True if a SysV segment for this key currently exists.
  bool SegmentExists() const
  {
    key_t key = ::ftok(path_.c_str(), 8);
    return key != -1 && ::shmget(key, 0, 0) >= 0;
  }

private:
  std::string path_;
};

// Fixed-layout payload used throughout the tests. `seq` / `ts` sit at the
// offsets the PacketProducer is told about so the library can stamp them.
struct Packet
{
  uint64_t seq = 0;     // written by ProduceWithSeqNumAndTimestamp
  int64_t ts = 0;       // written by ProduceWithSeqNumAndTimestamp
  uint32_t producer = 0;
  uint32_t index = 0;
  uint64_t checksum = 0;

  static constexpr size_t kSeqOffset = 0;
  static constexpr size_t kTsOffset = sizeof(uint64_t);

  void Stamp(uint32_t p, uint32_t i)
  {
    producer = p;
    index = i;
    checksum = Checksum();
  }
  uint64_t Checksum() const
  {
    return (uint64_t(producer) << 32 | index) * 0x9E3779B97F4A7C15ULL ^ 0xA5A5A5A5A5A5A5A5ULL;
  }
  bool Valid() const { return checksum == Checksum(); }
};
static_assert(sizeof(Packet) == 32);

// ShmContext keeps its sequence-number getters private (friends only), so
// tests observe them through the public cursor and ShmAdmin.
inline int64_t Committed(mcp::core::ShmContext& c) { return c.GetCursor(); }
inline int64_t Claimed(mcp::core::ShmContext& c) { return mcp::core::ShmAdmin(c).GetClaimedSequenceNumber(); }
inline std::vector<mcp::core::ConsumerStats> Stats(mcp::core::ShmContext& c)
{
  std::vector<mcp::core::ConsumerStats> out;
  mcp::core::ShmAdmin(c).GetConsumerStats(out);
  return out;
}

template<typename T>
inline const T& As(const mcp::core::BufferWrapper& buf)
{
  return *reinterpret_cast<const T*>(buf.data);
}

inline int64_t NowNs()
{
  using namespace std::chrono;
  return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

// Poll `pred` until true or `timeout` elapses.
template<typename Pred>
inline bool WaitUntil(Pred pred, std::chrono::milliseconds timeout)
{
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!pred()) {
    if (std::chrono::steady_clock::now() > deadline) return false;
    std::this_thread::yield();
  }
  return true;
}

} // namespace shmctx_test

// ---------------------------------------------------------------------------
// Big-memory tests
// ---------------------------------------------------------------------------
#include <catch2/catch_test_macros.hpp>

namespace shmctx_test {

// 1 GiB of storage with size_hint 512 -> 2M ledger slots (80 MiB): ~1.08 GiB
// per queue. With size_hint 64 the ledger alone would add 640 MiB.
constexpr int64_t kBigQueueBytes = 1LL << 30;
constexpr int64_t kBigSizeHint = 512;

inline int64_t MemAvailableBytes()
{
  std::FILE* f = std::fopen("/proc/meminfo", "r");
  if (!f) return -1;
  char line[256];
  int64_t kb = -1;
  while (std::fgets(line, sizeof(line), f))
    if (std::sscanf(line, "MemAvailable: %ld kB", &kb) == 1) break;
  std::fclose(f);
  return kb < 0 ? -1 : kb * 1024;
}

// Skip (not fail) when the machine cannot hold `bytes` of fresh pages. Set
// SHMCTX_FORCE_BIG=1 to run regardless.
#define SHMCTX_REQUIRE_MEMORY(bytes)                                                                          \
  do {                                                                                                      \
    int64_t avail_ = ::shmctx_test::MemAvailableBytes();                                                    \
    if (!std::getenv("SHMCTX_FORCE_BIG") && avail_ >= 0 && avail_ < (bytes)) {                              \
      SKIP("needs " << ((bytes) >> 20) << " MiB, only " << (avail_ >> 20) << " MiB available");             \
    }                                                                                                       \
  } while (0)

} // namespace shmctx_test
