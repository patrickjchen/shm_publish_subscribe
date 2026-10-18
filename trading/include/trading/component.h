#pragma once
// Component: base class for every process in the trading skeleton.
//
//   * holds references to the three buses (ShmContext, private or SysV)
//   * one ShmConsumer per subscribed bus, with a topic mask + owner filter
//   * one PacketProducer per bus it publishes on (seq/ts stamped at offset 0/8)
//   * a single-threaded event loop: poll every subscription, dispatch to
//     OnMessage(); call OnIdle() when nothing arrived; stop on RequestStop()
//     or after `idle_timeout` without traffic
//   * per-topic in/out counters for tests and the end-of-run summary
//
// Business logic belongs in subclasses' OnMessage(); this file only routes.

#include "shmctx/shmctx.h"
#include "shmctx/topic_mask.h"
#include "trading/messages.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>

namespace trading {

namespace mc = mcp::core;

struct Buses
{
  mc::ShmContext* client = nullptr;
  mc::ShmContext* order = nullptr;
  mc::ShmContext* md = nullptr;

  mc::ShmContext& Get(BusId id) const
  {
    mc::ShmContext* p = id == BusId::Client ? client : id == BusId::Order ? order : md;
    if (!p) throw std::runtime_error(std::string("bus not attached: ") + ToString(id));
    return *p;
  }
};

struct Envelope
{
  BusId bus;
  Topic topic;
  uint16_t dest;          // SHM strategy_id: owner / client id, 0 = broadcast
  const char* data;
  int64_t size;
  const Header& header() const { return *reinterpret_cast<const Header*>(data); }
  template<typename T> const T* As() const { return trading::As<T>(data, size); }
};

inline int64_t NowNs()
{
  using namespace std::chrono;
  return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

class Component
{
public:
  Component(std::string name, uint16_t src, const Buses& buses) : name_(std::move(name)), src_(src), buses_(buses) {}
  virtual ~Component() = default;
  Component(const Component&) = delete;
  Component& operator=(const Component&) = delete;

  const std::string& Name() const { return name_; }
  uint16_t Src() const { return src_; }

  // Stop after this long without any inbound message (0 = never).
  void SetIdleTimeout(std::chrono::milliseconds t) { idle_timeout_ = t; }
  void SetVerbose(bool v) { verbose_ = v; }
  void RequestStop() { stop_.store(true, std::memory_order_release); }
  bool StopRequested() const { return stop_.load(std::memory_order_acquire); }

  // One pass over every subscription: dispatch up to `batch` messages per bus
  // (bounded so one chatty bus cannot starve the others). Returns true if
  // anything was delivered. Usable stand-alone for drivers that interleave
  // their own work with message handling (see Client::Poll).
  bool PollOnce(int batch = 64)
  {
    bool any = false;
    for (auto& s : subs_) {
      for (int i = 0; i < batch; ++i) {
        auto buf = s.consumer->TryConsume(s.topics);
        if (buf.data == nullptr) break;
        any = true;
        Envelope env{ s.bus, Topic(buf.topic), buf.strategy_id, buf.data, buf.size };
        ++in_[buf.topic];
        if (verbose_) std::fprintf(stderr, "[%s] <- %s\n", name_.c_str(), Describe(env.bus, env.topic, env.dest, env.data, env.size).c_str());
        OnMessage(env);
      }
    }
    return any;
  }

  // Run until RequestStop() or idle timeout. `on_ready` fires once the loop
  // is about to start (subscriptions were created in the constructor).
  void Run(const std::function<void()>& on_ready = {})
  {
    OnStart();
    if (on_ready) on_ready();
    auto last_activity = std::chrono::steady_clock::now();
    uint32_t misses = 0;
    while (!StopRequested()) {
      bool any = PollOnce();
      auto now = std::chrono::steady_clock::now();
      if (any) {
        last_activity = now;
        misses = 0;
        continue;
      }
      OnIdle();
      if (idle_timeout_.count() > 0 && now - last_activity > idle_timeout_) {
        idle_timed_out_ = true;
        break;
      }
      if (++misses >= 128) { std::this_thread::yield(); misses = 0; }
    }
    // Graceful stop: drain whatever is already in the rings so a slow reader
    // (the persister) does not lose the tail of the stream. Bounded by the
    // moment a full pass finds nothing, so a still-chatty producer cannot
    // keep us alive forever: upstream is stopped first (see run_system.sh).
    if (drain_on_stop_ && !idle_timed_out_) {
      auto drain_deadline = std::chrono::steady_clock::now() + drain_timeout_;
      while (PollOnce(1024) && std::chrono::steady_clock::now() < drain_deadline) {}
    }
    OnStop();
  }

  // Whether Run() drains pending messages after RequestStop() (default on, max 2 s).
  void SetDrainOnStop(bool on, std::chrono::milliseconds max = std::chrono::seconds(2)) { drain_on_stop_ = on; drain_timeout_ = max; }

  bool IdleTimedOut() const { return idle_timed_out_; }

  uint64_t InCount(Topic t) const { return in_[t]; }
  uint64_t OutCount(Topic t) const { return out_[t]; }
  uint64_t InTotal() const { uint64_t n = 0; for (auto v : in_) n += v; return n; }
  uint64_t OutTotal() const { uint64_t n = 0; for (auto v : out_) n += v; return n; }

  std::string Summary() const
  {
    std::string s = name_ + ": in=" + std::to_string(InTotal()) + " out=" + std::to_string(OutTotal());
    for (size_t t = 0; t < in_.size(); ++t)
      if (in_[t]) s += " in." + std::string(ToString(Topic(t))) + "=" + std::to_string(in_[t]);
    for (size_t t = 0; t < out_.size(); ++t)
      if (out_[t]) s += " out." + std::string(ToString(Topic(t))) + "=" + std::to_string(out_[t]);
    return s;
  }

protected:
  // ---- wiring (call from the subclass constructor) ----

  // Subscribe to `topics` on `bus`; `owners` filters by SHM strategy_id
  // (empty = only broadcast messages; {0} = everything).
  void Subscribe(BusId bus, const shmctx::TopicMask& topics, const std::set<uint16_t>& owners = { 0 })
  {
    auto& ctx = buses_.Get(bus);
    Subscription s;
    s.bus = bus;
    s.consumer = std::make_unique<mc::ShmConsumer>(ctx);
    s.topics.topics = topics;
    s.topics.set_strategy_ids(owners);
    subs_.push_back(std::move(s));
  }

  // Publish `msg` on `bus` under `topic`, addressed to `dest` (0 = broadcast).
  template<typename T>
  void Publish(BusId bus, T& msg, Topic topic, uint16_t dest = 0)
  {
    msg.h.src = src_;
    msg.h.len = sizeof(T);
    Producer(bus).ProduceWithTopicAndStrategyId(reinterpret_cast<char*>(&msg), int64_t(sizeof(T)), uint16_t(topic), dest);
    ++out_[topic];
    if (verbose_) std::fprintf(stderr, "[%s] -> %s\n", name_.c_str(), Describe(bus, topic, dest, reinterpret_cast<const char*>(&msg), sizeof(T)).c_str());
  }

  // Monotonic id unique per component: top 16 bits = src, rest = counter.
  uint64_t NextId() { return (uint64_t(src_) << 48) | ++id_counter_; }

  // ---- hooks ----
  virtual void OnMessage(const Envelope& env) = 0;
  virtual void OnIdle() {}
  virtual void OnStart() {}
  virtual void OnStop() {}

  void Log(const char* fmt, ...) const __attribute__((format(printf, 2, 3)))
  {
    va_list ap;
    va_start(ap, fmt);
    std::fprintf(stderr, "[%s] ", name_.c_str());
    std::vfprintf(stderr, fmt, ap);
    std::fputc('\n', stderr);
    va_end(ap);
  }

  const Buses& buses() const { return buses_; }

private:
  struct Subscription
  {
    BusId bus;
    std::unique_ptr<mc::ShmConsumer> consumer;
    mc::SubscribedTopics topics;
  };

  mc::PacketProducer& Producer(BusId bus)
  {
    auto& p = producers_[size_t(bus)];
    if (!p) p = std::make_unique<mc::PacketProducer>(buses_.Get(bus), kSeqOffset, kTsOffset);
    return *p;
  }

  std::string name_;
  uint16_t src_;
  Buses buses_;
  std::vector<Subscription> subs_;
  std::array<std::unique_ptr<mc::PacketProducer>, 3> producers_;
  std::array<uint64_t, 64> in_{};
  std::array<uint64_t, 64> out_{};
  uint64_t id_counter_ = 0;
  std::atomic<bool> stop_{ false };
  std::chrono::milliseconds idle_timeout_{ 0 };
  bool idle_timed_out_ = false;
  bool verbose_ = false;
  bool drain_on_stop_ = true;
  std::chrono::milliseconds drain_timeout_{ 2000 };
};

} // namespace trading
