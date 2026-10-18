#pragma once
// Task layer: a configurable node that runs as a producer, a consumer, or a
// relay (consumer + producer in one process) over ShmContext queues.
//
//   producer : publishes `count` TaskMessages on `topic`, then one End marker
//   consumer : subscribes to a topic mask, verifies checksums + per-origin
//              ordering, stops after `ends` End markers (or `expect` messages)
//   relay    : consumes from `in` with a topic mask, transforms (hops++,
//              relay_id, re-seal), republishes on `out` / `out_topic`; forwards
//              End markers and stops after seeing `ends` of them
//
// Everything here is header-only and works on ShmContext in either private
// (single process, threads) or SysV shared-memory mode.

#include "shmctx/byte_size.h"
#include "shmctx/ini.h"
#include "shmctx/shmctx.h"
#include "shmctx/topic_mask.h"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace shmctx {

/////////////////////////////////////////////////////////////////////////////
// Wire message
/////////////////////////////////////////////////////////////////////////////

struct TaskMessage
{
  enum Kind : uint16_t { kData = 0, kEnd = 1 };

  uint64_t seq = 0;        // stamped by PacketProducer at the last hop
  int64_t ts = 0;          // stamped by PacketProducer at the last hop (ns)
  int64_t origin_ts = 0;   // set once by the producer (ns)
  uint32_t origin = 0;     // producer id
  uint32_t index = 0;      // per-producer counter
  uint16_t kind = kData;
  uint16_t hops = 0;       // incremented by every relay
  uint32_t relay_id = 0;   // id of the last relay (0 = none)
  uint32_t size = 0;       // total bytes incl. padding
  uint32_t pad_byte = 0xA5;
  uint64_t checksum = 0;

  static constexpr size_t kSeqOffset = 0;
  static constexpr size_t kTsOffset = sizeof(uint64_t);

  // Covers every field except seq / ts (hop-local) and checksum itself.
  uint64_t Compute() const
  {
    uint64_t h = 1469598103934665603ULL;
    auto mix = [&](uint64_t v) { h ^= v; h *= 1099511628211ULL; };
    mix(uint64_t(origin_ts)); mix(origin); mix(index); mix(kind); mix(hops); mix(relay_id); mix(size); mix(pad_byte);
    return h;
  }
  void Seal() { checksum = Compute(); }
  bool Valid() const { return checksum == Compute(); }
};
static_assert(sizeof(TaskMessage) == 56);
static_assert(std::is_trivially_copyable_v<TaskMessage>);

inline int64_t TaskNowNs()
{
  using namespace std::chrono;
  return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

/////////////////////////////////////////////////////////////////////////////
// Specs
/////////////////////////////////////////////////////////////////////////////

enum class TaskType { Producer, Consumer, Relay };

inline const char* ToString(TaskType t)
{
  switch (t) {
    case TaskType::Producer: return "producer";
    case TaskType::Consumer: return "consumer";
    case TaskType::Relay: return "relay";
  }
  return "?";
}

inline TaskType ParseTaskType(std::string_view s)
{
  if (s == "producer") return TaskType::Producer;
  if (s == "consumer") return TaskType::Consumer;
  if (s == "relay") return TaskType::Relay;
  throw std::invalid_argument("unknown task type '" + std::string(s) + "' (producer|consumer|relay)");
}

struct QueueSpec
{
  std::string name;
  std::string key;             // ftok() file path
  int64_t queue_size = 1 << 20;
  int64_t consumers = 8;
  int64_t size_hint = 64;
  bool create = false;         // pipeline runner creates (and destroys) it
};

// Bytes a queue occupies once created: storage (rounded to a power of two) +
// ledger slots (storage / pow2(size_hint), 40 bytes each) + 64 B per consumer
// + header. A "1g" queue with size_hint 64 is ~1.6 GiB; with size_hint 512 ~1.1 GiB.
inline int64_t EstimateQueueBytes(const QueueSpec& q)
{
  auto pow2 = [](int64_t v) { int64_t p = 1; while (p < v) p <<= 1; return p; };
  constexpr int64_t kLedgerSlotBytes = 40;   // sizeof(ShmContext::Metadata), which is private
  int64_t storage = pow2(q.queue_size);
  int64_t ledger = storage / pow2(q.size_hint);
  return 256 + q.consumers * 64 + ledger * kLedgerSlotBytes + storage;
}

struct TaskSpec
{
  std::string name;
  TaskType type = TaskType::Producer;

  // Queue references: names of [queue X] sections, or raw key-file paths.
  std::string queue;           // producer / consumer
  std::string in, out;         // relay

  uint32_t id = 0;             // producer: origin id; relay: relay_id
  uint16_t topic = 0;          // producer publish topic
  uint16_t out_topic = 0;      // relay publish topic
  uint16_t strategy_id = 0;    // publish strategy id (producer + relay)

  TopicMask subscribe = TopicMask().set();    // consumer / relay input filter
  std::set<uint16_t> strategy_ids;            // consumer / relay strategy filter ({} = broadcast only)

  uint32_t count = 1000;       // producer: messages to send
  uint32_t size = sizeof(TaskMessage);   // producer: bytes per message (>= header)
  uint32_t pause_us = 0;       // producer: sleep between messages
  uint32_t expect = 0;         // consumer: stop after this many data msgs (0 = use ends)
  uint32_t ends = 1;           // consumer / relay: End markers to wait for
  bool check_order = true;     // consumer: require per-origin index == previous + 1
  int timeout_s = 30;          // consumer / relay: give up after this
  bool verbose = false;

  bool IsSource() const { return type == TaskType::Producer; }
};

struct PipelineSpec
{
  std::vector<QueueSpec> queues;
  std::vector<TaskSpec> tasks;
  int ready_timeout_s = 10;

  const QueueSpec* FindQueue(std::string_view name) const
  {
    for (auto& q : queues)
      if (q.name == name) return &q;
    return nullptr;
  }
  // Queue reference -> key path. Unknown names are taken literally as paths.
  std::string ResolveKey(const std::string& ref) const
  {
    if (auto* q = FindQueue(ref)) return q->key;
    return ref;
  }
  const TaskSpec* FindTask(std::string_view name) const
  {
    for (auto& t : tasks)
      if (t.name == name) return &t;
    return nullptr;
  }
};

/////////////////////////////////////////////////////////////////////////////
// Spec construction from key/value maps (INI sections or CLI flags)
/////////////////////////////////////////////////////////////////////////////

namespace detail {
inline uint64_t ToUnsigned(const std::string& key, const std::string& v, uint64_t max)
{
  size_t idx = 0;
  long long n = std::stoll(v, &idx, 0);
  if (idx != v.size() || n < 0 || uint64_t(n) > max) throw std::invalid_argument(key + ": invalid value '" + v + "'");
  return uint64_t(n);
}
inline bool ToBool(const std::string& key, std::string v)
{
  for (auto& c : v) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  if (v == "1" || v == "true" || v == "yes" || v == "on") return true;
  if (v == "0" || v == "false" || v == "no" || v == "off") return false;
  throw std::invalid_argument(key + ": not a boolean '" + v + "'");
}
} // namespace detail

// Apply one key=value to a TaskSpec. Keys accept '-' or '_' interchangeably
// so CLI flags (--out-topic) and INI keys (out_topic) share one vocabulary.
inline void ApplyTaskKey(TaskSpec& t, std::string key, const std::string& value)
{
  for (auto& c : key) if (c == '-') c = '_';
  using detail::ToBool; using detail::ToUnsigned;
  if (key == "name") t.name = value;
  else if (key == "type") t.type = ParseTaskType(value);
  else if (key == "queue") t.queue = value;
  else if (key == "in") t.in = value;
  else if (key == "out") t.out = value;
  else if (key == "id") t.id = uint32_t(ToUnsigned(key, value, UINT32_MAX));
  else if (key == "topic") t.topic = uint16_t(ToUnsigned(key, value, 63));
  else if (key == "out_topic") t.out_topic = uint16_t(ToUnsigned(key, value, 63));
  else if (key == "strategy_id") t.strategy_id = uint16_t(ToUnsigned(key, value, UINT16_MAX));
  else if (key == "subscribe") t.subscribe = ParseTopicMask(value);
  else if (key == "strategy_ids") t.strategy_ids = ParseIdList(value);
  else if (key == "count") t.count = uint32_t(ToUnsigned(key, value, UINT32_MAX));
  else if (key == "size") t.size = uint32_t(ParseByteSize(value));
  else if (key == "pause_us") t.pause_us = uint32_t(ToUnsigned(key, value, UINT32_MAX));
  else if (key == "expect") t.expect = uint32_t(ToUnsigned(key, value, UINT32_MAX));
  else if (key == "ends") t.ends = uint32_t(ToUnsigned(key, value, UINT32_MAX));
  else if (key == "check_order") t.check_order = ToBool(key, value);
  else if (key == "timeout_s") t.timeout_s = int(ToUnsigned(key, value, INT32_MAX));
  else if (key == "verbose") t.verbose = ToBool(key, value);
  else throw std::invalid_argument("unknown task key '" + key + "'");
}

inline void ApplyQueueKey(QueueSpec& q, std::string key, const std::string& value)
{
  for (auto& c : key) if (c == '-') c = '_';
  using detail::ToBool; using detail::ToUnsigned;
  if (key == "name") q.name = value;
  else if (key == "key") q.key = value;
  else if (key == "queue_size") q.queue_size = ParseByteSize(value);      // "2g", "256m", 1048576
  else if (key == "consumers") q.consumers = int64_t(ToUnsigned(key, value, 4096));
  else if (key == "size_hint") q.size_hint = ParseByteSize(value);
  else if (key == "create") q.create = ToBool(key, value);
  else throw std::invalid_argument("unknown queue key '" + key + "'");
}

inline void ValidateTask(const TaskSpec& t)
{
  auto fail = [&](const std::string& m) { throw std::invalid_argument("task '" + t.name + "': " + m); };
  switch (t.type) {
    case TaskType::Producer:
      if (t.queue.empty()) fail("producer needs 'queue'");
      if (t.size < sizeof(TaskMessage)) fail("size must be >= " + std::to_string(sizeof(TaskMessage)));
      break;
    case TaskType::Consumer:
      if (t.queue.empty()) fail("consumer needs 'queue'");
      if (t.subscribe.none()) fail("subscribe mask is empty");
      break;
    case TaskType::Relay:
      if (t.in.empty() || t.out.empty()) fail("relay needs 'in' and 'out'");
      if (t.subscribe.none()) fail("subscribe mask is empty");
      if (t.in == t.out && t.subscribe.test(t.out_topic))
        fail("relay reads and writes the same queue and its subscribe mask includes out_topic " + std::to_string(t.out_topic) + " (would loop forever)");
      break;
  }
}

inline PipelineSpec LoadPipeline(const IniFile& ini)
{
  PipelineSpec p;
  for (auto* s : ini.SectionsOfKind("queue")) {
    QueueSpec q;
    q.name = s->name;
    for (auto& [k, v] : s->values) ApplyQueueKey(q, k, v);
    if (q.key.empty()) throw std::invalid_argument("[queue " + q.name + "] needs 'key'");
    if (p.FindQueue(q.name)) throw std::invalid_argument("duplicate queue '" + q.name + "'");
    p.queues.push_back(std::move(q));
  }
  for (auto* s : ini.SectionsOfKind("task")) {
    TaskSpec t;
    t.name = s->name;
    // 'type' first so defaults can depend on it later if needed.
    if (!s->Has("type")) throw std::invalid_argument("[task " + t.name + "] needs 'type'");
    ApplyTaskKey(t, "type", s->Get("type"));
    for (auto& [k, v] : s->values)
      if (k != "type") ApplyTaskKey(t, k, v);
    ValidateTask(t);
    if (p.FindTask(t.name)) throw std::invalid_argument("duplicate task '" + t.name + "'");
    p.tasks.push_back(std::move(t));
  }
  if (auto* g = ini.Find("", "pipeline")) p.ready_timeout_s = int(g->GetInt("ready_timeout_s", p.ready_timeout_s));
  else if (auto* g2 = ini.Find("pipeline", "")) p.ready_timeout_s = int(g2->GetInt("ready_timeout_s", p.ready_timeout_s));
  return p;
}

inline std::string DescribeTask(const TaskSpec& t)
{
  std::string s = t.name + " [" + ToString(t.type) + "]";
  switch (t.type) {
    case TaskType::Producer:
      s += " queue=" + t.queue + " id=" + std::to_string(t.id) + " topic=" + std::to_string(t.topic)
         + " strategy_id=" + std::to_string(t.strategy_id) + " count=" + std::to_string(t.count) + " size=" + std::to_string(t.size);
      break;
    case TaskType::Consumer:
      s += " queue=" + t.queue + " subscribe=" + FormatTopicMask(t.subscribe) + " ends=" + std::to_string(t.ends)
         + " expect=" + std::to_string(t.expect) + " timeout_s=" + std::to_string(t.timeout_s);
      break;
    case TaskType::Relay:
      s += " in=" + t.in + " subscribe=" + FormatTopicMask(t.subscribe) + " -> out=" + t.out + " out_topic=" + std::to_string(t.out_topic)
         + " id=" + std::to_string(t.id) + " ends=" + std::to_string(t.ends);
      break;
  }
  if (!t.strategy_ids.empty()) {
    s += " strategy_ids={";
    bool first = true;
    for (auto id : t.strategy_ids) { if (!first) s += ','; first = false; s += std::to_string(id); }
    s += "}";
  }
  return s;
}

/////////////////////////////////////////////////////////////////////////////
// Runtime
/////////////////////////////////////////////////////////////////////////////

struct TaskStats
{
  std::string name;
  TaskType type = TaskType::Producer;
  uint64_t produced = 0;      // producer + relay (data msgs published)
  uint64_t received = 0;      // consumer + relay (data msgs consumed)
  uint64_t bad = 0;           // checksum failures
  uint64_t misordered = 0;    // per-origin ordering violations
  uint64_t ends_seen = 0;
  uint64_t max_hops = 0;
  int64_t max_lag = 0;        // max (queue head - our cursor) observed; size the ledger above this
  uint64_t overruns = 0;      // messages read while lag >= ledger size: slot may have been overwritten
  int64_t max_latency_ns = 0; // origin_ts -> consume (consumer / relay)
  int64_t elapsed_ns = 0;
  bool timed_out = false;
  bool ok = false;
  std::map<uint32_t, uint32_t> per_origin;   // data msgs per origin id

  std::string ToString() const
  {
    char buf[512];
    std::snprintf(buf, sizeof(buf),
                  "%s[%s] ok=%d produced=%llu received=%llu bad=%llu misordered=%llu ends=%llu origins=%zu max_hops=%llu max_lag=%lld overruns=%llu max_latency_us=%.1f elapsed_ms=%.2f%s%s",
                  name.c_str(), shmctx::ToString(type), ok ? 1 : 0, (unsigned long long)produced, (unsigned long long)received,
                  (unsigned long long)bad, (unsigned long long)misordered, (unsigned long long)ends_seen, per_origin.size(),
                  (unsigned long long)max_hops, (long long)max_lag, (unsigned long long)overruns, max_latency_ns / 1000.0, elapsed_ns / 1e6,
                  timed_out ? " TIMEOUT" : "", overruns ? " OVERRUN(ledger too small for consumer lag)" : "");
    return buf;
  }
};

using ReadyCallback = std::function<void()>;

namespace detail {
inline mcp::core::SubscribedTopics MakeSubscription(const TaskSpec& t)
{
  mcp::core::SubscribedTopics subs;
  subs.topics = t.subscribe;
  subs.set_strategy_ids(t.strategy_ids);
  return subs;
}

// Verify + account for one consumed message; returns the message or nullptr if too small.
inline const TaskMessage* Account(const mcp::core::BufferWrapper& buf, const TaskSpec& t, TaskStats& st,
                                  std::map<uint32_t, uint32_t>& next_index)
{
  if (buf.size < int64_t(sizeof(TaskMessage))) { ++st.bad; return nullptr; }
  const auto* m = reinterpret_cast<const TaskMessage*>(buf.data);
  if (!m->Valid() || m->size != uint32_t(buf.size)) ++st.bad;
  if (m->kind == TaskMessage::kEnd) { ++st.ends_seen; return m; }
  ++st.received;
  ++st.per_origin[m->origin];
  if (t.check_order) {
    auto it = next_index.find(m->origin);
    if (it == next_index.end()) next_index[m->origin] = m->index + 1;
    else { if (m->index != it->second) ++st.misordered; it->second = m->index + 1; }
  }
  st.max_hops = std::max<uint64_t>(st.max_hops, m->hops);
  st.max_latency_ns = std::max(st.max_latency_ns, TaskNowNs() - m->origin_ts);
  return m;
}

// After a successful TryConsume: how far behind the head are we? If the gap
// reaches the ledger size the slot we just read may already be overwritten
// (the queue has no back-pressure), so flag it.
inline void TrackLag(const mcp::core::ShmConsumer& consumer, int64_t ledger_size, TaskStats& st)
{
  auto [next, head] = consumer.GetCursor();
  int64_t lag = head - (next - 1);
  if (lag > st.max_lag) st.max_lag = lag;
  if (lag >= ledger_size) ++st.overruns;
}

inline void Pause(uint32_t& misses)
{
  if (++misses >= 256) { std::this_thread::yield(); misses = 0; }
}
} // namespace detail

inline TaskStats RunProducer(mcp::core::ShmContext& q, const TaskSpec& t, const ReadyCallback& on_ready = {})
{
  TaskStats st; st.name = t.name; st.type = TaskType::Producer;
  auto t0 = std::chrono::steady_clock::now();
  mcp::core::PacketProducer producer(q, TaskMessage::kSeqOffset, TaskMessage::kTsOffset);
  std::vector<char> frame(t.size, char(0xA5));
  auto* msg = reinterpret_cast<TaskMessage*>(frame.data());
  if (on_ready) on_ready();

  for (uint32_t i = 0; i < t.count; ++i) {
    *msg = TaskMessage{};
    msg->origin = t.id; msg->index = i; msg->kind = TaskMessage::kData; msg->size = t.size; msg->origin_ts = TaskNowNs();
    msg->Seal();
    producer.ProduceWithTopicAndStrategyId(frame.data(), int64_t(t.size), t.topic, t.strategy_id);
    ++st.produced;
    if (t.pause_us) std::this_thread::sleep_for(std::chrono::microseconds(t.pause_us));
  }
  // End marker: broadcast strategy so every filtered consumer sees it.
  *msg = TaskMessage{};
  msg->origin = t.id; msg->index = t.count; msg->kind = TaskMessage::kEnd; msg->size = uint32_t(sizeof(TaskMessage)); msg->origin_ts = TaskNowNs();
  msg->Seal();
  producer.ProduceWithTopicAndStrategyId(frame.data(), int64_t(sizeof(TaskMessage)), t.topic, 0);
  ++st.ends_seen;

  st.elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
  st.ok = true;
  return st;
}

inline TaskStats RunConsumer(mcp::core::ShmContext& q, const TaskSpec& t, const ReadyCallback& on_ready = {})
{
  TaskStats st; st.name = t.name; st.type = TaskType::Consumer;
  auto subs = detail::MakeSubscription(t);
  mcp::core::ShmConsumer consumer(q);
  if (on_ready) on_ready();

  auto t0 = std::chrono::steady_clock::now();
  auto deadline = t0 + std::chrono::seconds(t.timeout_s);
  std::map<uint32_t, uint32_t> next_index;
  uint32_t misses = 0;
  auto done = [&] { return (t.expect > 0 && st.received >= t.expect) || (t.expect == 0 && st.ends_seen >= t.ends); };
  while (!done()) {
    auto buf = consumer.TryConsume(subs);
    if (buf.data == nullptr) {
      if (std::chrono::steady_clock::now() > deadline) { st.timed_out = true; break; }
      detail::Pause(misses);
      continue;
    }
    misses = 0;
    detail::TrackLag(consumer, q.GetLedgerSize(), st);
    if (auto* m = detail::Account(buf, t, st, next_index); m && t.verbose)
      std::fprintf(stderr, "[%s] %s origin=%u index=%u hops=%u relay=%u topic=%u strat=%u\n", t.name.c_str(),
                   m->kind == TaskMessage::kEnd ? "END " : "data", m->origin, m->index, m->hops, m->relay_id, buf.topic, buf.strategy_id);
  }
  st.elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
  st.ok = !st.timed_out && st.bad == 0 && (!t.check_order || st.misordered == 0) && (t.expect == 0 || st.received == t.expect);
  return st;
}

inline TaskStats RunRelay(mcp::core::ShmContext& in, mcp::core::ShmContext& out, const TaskSpec& t, const ReadyCallback& on_ready = {})
{
  TaskStats st; st.name = t.name; st.type = TaskType::Relay;
  auto subs = detail::MakeSubscription(t);
  mcp::core::ShmConsumer consumer(in);
  mcp::core::PacketProducer producer(out, TaskMessage::kSeqOffset, TaskMessage::kTsOffset);
  std::vector<char> frame;
  if (on_ready) on_ready();

  auto t0 = std::chrono::steady_clock::now();
  auto deadline = t0 + std::chrono::seconds(t.timeout_s);
  std::map<uint32_t, uint32_t> next_index;
  uint32_t misses = 0;
  while (st.ends_seen < t.ends) {
    auto buf = consumer.TryConsume(subs);
    if (buf.data == nullptr) {
      if (std::chrono::steady_clock::now() > deadline) { st.timed_out = true; break; }
      detail::Pause(misses);
      continue;
    }
    misses = 0;
    detail::TrackLag(consumer, in.GetLedgerSize(), st);
    const TaskMessage* m = detail::Account(buf, t, st, next_index);
    if (!m) continue;

    // Transform: copy out of the ring (the slot may be overwritten while we
    // publish), bump hop count, tag relay, re-seal, forward.
    frame.assign(buf.data, buf.data + buf.size);
    auto* fwd = reinterpret_cast<TaskMessage*>(frame.data());
    fwd->hops = uint16_t(m->hops + 1);
    fwd->relay_id = t.id;
    fwd->Seal();
    uint16_t strat = (fwd->kind == TaskMessage::kEnd) ? uint16_t(0) : t.strategy_id;
    producer.ProduceWithTopicAndStrategyId(frame.data(), int64_t(frame.size()), t.out_topic, strat);
    if (fwd->kind == TaskMessage::kData) ++st.produced;
    if (t.verbose)
      std::fprintf(stderr, "[%s] fwd %s origin=%u index=%u hops=%u -> topic=%u\n", t.name.c_str(),
                   fwd->kind == TaskMessage::kEnd ? "END " : "data", fwd->origin, fwd->index, fwd->hops, t.out_topic);
  }
  st.elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
  st.ok = !st.timed_out && st.bad == 0 && (!t.check_order || st.misordered == 0);
  return st;
}

// Dispatch on type. `resolve` maps a queue reference (name or path) to an
// attached/created ShmContext; it is called once per distinct reference.
inline TaskStats RunTask(const TaskSpec& t, const std::function<mcp::core::ShmContext&(const std::string&)>& resolve,
                         const ReadyCallback& on_ready = {})
{
  ValidateTask(t);
  switch (t.type) {
    case TaskType::Producer: return RunProducer(resolve(t.queue), t, on_ready);
    case TaskType::Consumer: return RunConsumer(resolve(t.queue), t, on_ready);
    case TaskType::Relay: {
      auto& in = resolve(t.in);
      auto& out = (t.in == t.out) ? in : resolve(t.out);
      return RunRelay(in, out, t, on_ready);
    }
  }
  throw std::logic_error("unreachable");
}

} // namespace shmctx
