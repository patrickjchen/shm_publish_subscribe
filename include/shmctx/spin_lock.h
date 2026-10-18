#pragma once

#include <time.h>

#include <atomic>
#include <memory>
#include <vector>

#include "shmctx/macros.h"

namespace mcp::core {

class AtomicFlagSpinlock
{
  enum { MAX_SPINS = 5 };

  std::atomic_flag barrier_ = ATOMIC_FLAG_INIT;
  const size_t max_spins_ = MAX_SPINS;
  struct timespec sleep_time_;

public:
  // Prevent moving and copying
  DISALLOW_COPY_AND_MOVE(AtomicFlagSpinlock)

  AtomicFlagSpinlock()
  {
    sleep_time_.tv_sec = 0;
    sleep_time_.tv_nsec = 1; // 1 nsec
  }

  AtomicFlagSpinlock(size_t nsec, size_t maxSpins)
    : max_spins_(maxSpins)
  {
    sleep_time_.tv_sec = 0;
    sleep_time_.tv_nsec = nsec;
  }

  ~AtomicFlagSpinlock() { unlock(); }

  void lock()
  {
    size_t cnt = max_spins_;
    while (barrier_.test_and_set(std::memory_order_acquire)) {
      --cnt;
      if (0 == cnt) {
        nanosleep(&sleep_time_, 0);
        cnt = max_spins_;
      }
    }
  }

  void unlock() { barrier_.clear(std::memory_order_release); }
};

} // namespace mcp::core