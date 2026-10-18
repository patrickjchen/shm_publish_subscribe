#pragma once
// Algo engine: owns algo (parent) orders and slices them into children.
//   order_bus kAlgoOrder (OrderRequest) -> N x ChildOrder -> order_bus kEmsRequest (owner = kOwnerAlgo)
//   order_bus kAlgoOrder (ChildCancel)  -> ChildCancel per live child -> kEmsRequest
//   order_bus kExecReport dest kOwnerAlgo -> aggregate to parent -> ClientReport -> kOwnerReport
//   md_bus    kBookTicker / kTrade        -> OnMarketData() (where TWAP/VWAP/POV would decide)
// Routing only: Slice() emits a single child for the whole parent quantity.
#include "trading/component.h"

#include <unordered_map>
#include <vector>

namespace trading {

class AlgoEngine : public Component
{
public:
  explicit AlgoEngine(const Buses& buses) : Component("algo_engine", kSrcAlgo, buses)
  {
    Subscribe(BusId::Order, shmctx::TopicMask().set(kAlgoOrder).set(kExecReport), { kOwnerAlgo });
    if (buses.md) Subscribe(BusId::Md, shmctx::TopicMask().set(kBookTicker).set(kTrade));
  }

  size_t OpenParents() const { return parents_.size(); }
  uint64_t MarketDataSeen() const { return md_seen_; }

protected:
  struct Parent
  {
    OrderRequest req;
    std::vector<uint64_t> children;
    double cum_qty = 0;
    double leaves_qty = 0;
  };

  void OnMessage(const Envelope& env) override
  {
    switch (env.header().type) {
      case MsgType::OrderRequest: {
        auto* req = env.As<OrderRequest>();
        if (!req) return;
        Parent& p = parents_[req->order_id];
        p.req = *req;
        p.leaves_qty = req->qty;
        for (ChildOrder& child : Slice(*req)) {
          child.order_id = req->order_id;
          child.child_id = NextId();
          child.client_id = req->client_id;
          child.owner = kOwnerAlgo;
          child_to_parent_[child.child_id] = req->order_id;
          p.children.push_back(child.child_id);
          Publish(BusId::Order, child, kEmsRequest);
        }
        break;
      }
      case MsgType::ChildCancel: {
        auto* c = env.As<ChildCancel>();
        if (!c) return;
        auto it = parents_.find(c->order_id);
        if (it == parents_.end()) return;
        for (uint64_t child_id : it->second.children) {
          ChildCancel out;
          out.order_id = c->order_id;
          out.child_id = child_id;
          out.owner = kOwnerAlgo;
          Publish(BusId::Order, out, kEmsRequest);
        }
        break;
      }
      case MsgType::ExecReport: {
        auto* er = env.As<ExecReport>();
        if (!er) return;
        auto pit = parents_.find(er->order_id);
        if (pit == parents_.end()) return;
        Parent& p = pit->second;
        p.cum_qty += er->fill_qty;
        p.leaves_qty = p.req.qty - p.cum_qty;
        if (IsTerminal(er->status)) {
          child_to_parent_.erase(er->child_id);
          auto& ch = p.children;
          ch.erase(std::remove(ch.begin(), ch.end(), er->child_id), ch.end());
        }
        // Parent status: terminal only once every child is done.
        ExecStatus status = er->status;
        if (status == ExecStatus::Filled && !ch_empty(p)) status = ExecStatus::PartiallyFilled;
        if (status == ExecStatus::Canceled && !ch_empty(p)) status = ExecStatus::PartiallyFilled;
        ClientReport rep;
        rep.client_id = p.req.client_id;
        rep.client_order_id = p.req.client_order_id;
        rep.order_id = er->order_id;
        rep.status = status;
        rep.fill_qty = er->fill_qty;
        rep.fill_px = er->fill_px;
        rep.cum_qty = p.cum_qty;
        rep.leaves_qty = p.leaves_qty;
        Publish(BusId::Order, rep, kOwnerReport);
        if (IsTerminal(status)) parents_.erase(pit);
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

  // Business-logic hooks.
  // Slice: decide the child orders for a new parent. Skeleton: one child for everything.
  virtual std::vector<ChildOrder> Slice(const OrderRequest& req)
  {
    ChildOrder child;
    child.side = req.side;
    child.symbol = req.symbol;
    child.qty = req.qty;
    child.px = req.px;
    return { child };
  }
  // Market data callback: a real TWAP/VWAP/POV would schedule further children here.
  virtual void OnMarketData(const Envelope&) {}

private:
  static bool ch_empty(const Parent& p) { return p.children.empty(); }
  std::unordered_map<uint64_t, Parent> parents_;
  std::unordered_map<uint64_t, uint64_t> child_to_parent_;
  uint64_t md_seen_ = 0;
};

} // namespace trading
