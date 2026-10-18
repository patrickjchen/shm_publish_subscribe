#pragma once

#include <cstdint>

namespace mcp::core {

struct BufferWrapper
{
  char* data;
  int64_t size;
  uint16_t topic = 0;
  uint16_t strategy_id = 0;
};

// Reserved topic value used to mark a ledger slot whose claiming producer died
// before committing. Consumers must treat this as "skip" rather than dispatch.
// 0xFFFF is outside the addressable range of the std::bitset<64> used in
// SubscribedTopics, so it is unreachable via normal topic registration.
constexpr uint16_t kSkipTopicSentinel = 0xFFFF;

} // namespace mcp::core
