#pragma once
// Persister: append-only journal of every order-flow message.
//   client_bus  all topics (orders in, reports out)
//   order_bus   all topics (requests, child orders, exchange traffic, exec reports)
//   md_bus      optional (`log_md`), off by default: volume
// One line per message (Describe() format), flushed every `flush_every`
// messages and on stop. A real persister would batch into a database; this
// is the routing-side hook for it.
#include "trading/component.h"

#include <fstream>
#include <string>

namespace trading {

class Persister : public Component
{
public:
  Persister(const Buses& buses, std::string path, bool log_md = false, uint32_t flush_every = 256)
    : Component("persister", kSrcPersister, buses), path_(std::move(path)), flush_every_(flush_every)
  {
    out_.open(path_, std::ios::out | std::ios::app);
    if (!out_) throw std::runtime_error("persister: cannot open " + path_);
    Subscribe(BusId::Client, shmctx::ParseTopicMask("all"), { 0 });   // {0}: every destination
    Subscribe(BusId::Order, shmctx::ParseTopicMask("all"), { 0 });
    if (log_md && buses.md) Subscribe(BusId::Md, shmctx::ParseTopicMask("all"), { 0 });
  }

  uint64_t Lines() const { return lines_; }
  const std::string& Path() const { return path_; }

protected:
  void OnMessage(const Envelope& env) override
  {
    out_ << Describe(env.bus, env.topic, env.dest, env.data, env.size) << '\n';
    if (++lines_ % flush_every_ == 0) out_.flush();
  }
  void OnStop() override { out_.flush(); }

private:
  std::string path_;
  std::ofstream out_;
  uint32_t flush_every_;
  uint64_t lines_ = 0;
};

} // namespace trading
