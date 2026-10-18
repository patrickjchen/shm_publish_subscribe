#pragma once
// Client: what an external order source looks like to the gateway.
//   publishes client_bus kClientOrder (NewOrder / CancelOrder)
//   subscribes client_bus kClientReport, dest = its client_id
// Tracks per-order status so a simulator/test can wait for completion.
#include "trading/component.h"

#include <map>

namespace trading {

class Client : public Component
{
public:
  Client(const Buses& buses, uint32_t client_id)
    : Component("client_" + std::to_string(client_id), uint16_t(kSrcClientBase + client_id), buses), client_id_(client_id)
  {
    Subscribe(BusId::Client, shmctx::TopicMask().set(kClientReport), { uint16_t(client_id) });
  }

  struct OrderState { ExecStatus status = ExecStatus::New; double cum_qty = 0; double qty = 0; uint32_t reports = 0; bool acked = false; };

  uint64_t SendOrder(const char* symbol, Side side, double qty, double px, AlgoType algo = AlgoType::None)
  {
    NewOrder o;
    o.client_id = client_id_;
    o.client_order_id = ++next_cl_ord_;
    o.symbol = Symbol(symbol);
    o.side = side;
    o.algo = algo;
    o.qty = qty;
    o.px = px;
    o.algo_duration_ns = algo == AlgoType::None ? 0 : 60'000'000'000LL;
    orders_[o.client_order_id] = OrderState{ ExecStatus::New, 0, qty, 0, false };
    Publish(BusId::Client, o, kClientOrder);
    return o.client_order_id;
  }

  void Cancel(uint64_t client_order_id)
  {
    CancelOrder c;
    c.client_id = client_id_;
    c.client_order_id = client_order_id;
    Publish(BusId::Client, c, kClientOrder);
  }

  // Non-blocking drain of pending reports; drivers interleave this with SendOrder().
  void Poll() { PollOnce(); }

  size_t Open() const
  {
    size_t n = 0;
    for (auto& [id, st] : orders_) if (!IsTerminal(st.status)) ++n;
    return n;
  }
  bool AllDone() const { return Open() == 0; }
  const std::map<uint64_t, OrderState>& Orders() const { return orders_; }
  uint32_t ClientId() const { return client_id_; }

protected:
  void OnMessage(const Envelope& env) override
  {
    auto* rep = env.As<ClientReport>();
    if (!rep || rep->client_id != client_id_) return;
    auto it = orders_.find(rep->client_order_id);
    if (it == orders_.end()) return;
    it->second.status = rep->status;
    it->second.cum_qty = rep->cum_qty;
    it->second.acked = true;
    ++it->second.reports;
  }

private:
  uint32_t client_id_;
  uint64_t next_cl_ord_ = 0;
  std::map<uint64_t, OrderState> orders_;
};

} // namespace trading
