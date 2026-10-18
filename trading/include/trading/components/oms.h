#pragma once
// OMS: owns simple (non-algo) orders.
//   order_bus kSimpleOrder (OrderRequest) -> ChildOrder -> order_bus kEmsRequest  (owner = kOwnerOms)
//   order_bus kSimpleOrder (ChildCancel)  -> ChildCancel -> order_bus kEmsRequest
//   order_bus kExecReport  dest kOwnerOms -> ClientReport -> order_bus kOwnerReport
//   md_bus    kBookTicker / kTrade        -> OnMarketData() (position marking, etc.)
// Routing only: one child per parent, no position/risk logic.
#include "trading/component.h"

#include <unordered_map>

namespace trading {

class Oms : public Component
{
public:
  explicit Oms(const Buses& buses) : Component("oms", kSrcOms, buses)
  {
    // One subscription on order_bus: SimpleOrder traffic is broadcast (dest 0),
    // ExecReports are addressed to kOwnerOms. Both pass the {kOwnerOms} filter.
    Subscribe(BusId::Order, shmctx::TopicMask().set(kSimpleOrder).set(kExecReport), { kOwnerOms });
    if (buses.md) Subscribe(BusId::Md, shmctx::TopicMask().set(kBookTicker).set(kTrade));
  }

  size_t OpenOrders() const { return orders_.size(); }
  uint64_t MarketDataSeen() const { return md_seen_; }

protected:
  void OnMessage(const Envelope& env) override
  {
    switch (env.header().type) {
      case MsgType::OrderRequest: {
        auto* req = env.As<OrderRequest>();
        if (!req) return;
        ChildOrder child;
        child.order_id = req->order_id;
        child.child_id = NextId();
        child.client_id = req->client_id;
        child.owner = kOwnerOms;
        child.side = req->side;
        child.symbol = req->symbol;
        child.qty = req->qty;
        child.px = req->px;
        orders_[req->order_id] = { *req, child.child_id, 0.0 };
        Publish(BusId::Order, child, kEmsRequest);
        break;
      }
      case MsgType::ChildCancel: {   // from the gateway: cancel the parent -> cancel its child
        auto* c = env.As<ChildCancel>();
        if (!c) return;
        auto it = orders_.find(c->order_id);
        if (it == orders_.end()) return;
        ChildCancel out;
        out.order_id = c->order_id;
        out.child_id = it->second.child_id;
        out.owner = kOwnerOms;
        Publish(BusId::Order, out, kEmsRequest);
        break;
      }
      case MsgType::ExecReport: {
        auto* er = env.As<ExecReport>();
        if (!er) return;
        auto it = orders_.find(er->order_id);
        if (it == orders_.end()) return;
        it->second.cum_qty += er->fill_qty;
        ClientReport rep;
        rep.client_id = it->second.req.client_id;
        rep.client_order_id = it->second.req.client_order_id;
        rep.order_id = er->order_id;
        rep.status = er->status;
        rep.fill_qty = er->fill_qty;
        rep.fill_px = er->fill_px;
        rep.cum_qty = it->second.cum_qty;
        rep.leaves_qty = er->leaves_qty;
        Publish(BusId::Order, rep, kOwnerReport);
        if (IsTerminal(er->status)) orders_.erase(it);
        break;
      }
      case MsgType::BookTicker:
      case MsgType::Trade:
        ++md_seen_;
        OnMarketData(env);
        break;
      default: break;
    }
  }

  // Business-logic hook (marking, risk, etc.).
  virtual void OnMarketData(const Envelope&) {}

private:
  struct Live { OrderRequest req; uint64_t child_id; double cum_qty; };
  std::unordered_map<uint64_t, Live> orders_;
  uint64_t md_seen_ = 0;
};

} // namespace trading
