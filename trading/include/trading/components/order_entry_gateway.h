#pragma once
// Order entry gateway: the clients' only contact point.
//   client_bus kClientOrder  -> assign order_id -> order_bus kSimpleOrder (algo == None)
//                                               -> order_bus kAlgoOrder   (algo != None)
//   order_bus  kOwnerReport  -> client_bus kClientReport, dest = client_id
// Hooks for real logic: ValidateOrder(), risk/throttle checks before routing.
#include "trading/component.h"

#include <unordered_map>

namespace trading {

class OrderEntryGateway : public Component
{
public:
  explicit OrderEntryGateway(const Buses& buses) : Component("gateway", kSrcGateway, buses)
  {
    Subscribe(BusId::Client, shmctx::ParseTopicMask("1"));                  // kClientOrder (any client)
    Subscribe(BusId::Order, shmctx::TopicMask().set(kOwnerReport));          // reports from OMS / algo
  }

  size_t LiveOrders() const { return orders_.size(); }

protected:
  void OnMessage(const Envelope& env) override
  {
    switch (env.header().type) {
      case MsgType::NewOrder: {
        auto* in = env.As<NewOrder>();
        if (!in) return;
        if (!ValidateOrder(*in)) { Reject(*in); return; }
        OrderRequest req;
        req.order_id = NextId();
        req.client_order_id = in->client_order_id;
        req.client_id = in->client_id;
        req.side = in->side;
        req.algo = in->algo;
        req.symbol = in->symbol;
        req.qty = in->qty;
        req.px = in->px;
        req.algo_duration_ns = in->algo_duration_ns;
        orders_[req.order_id] = { in->client_id, in->client_order_id, in->algo };
        by_client_[Key(in->client_id, in->client_order_id)] = req.order_id;
        Publish(BusId::Order, req, in->algo == AlgoType::None ? kSimpleOrder : kAlgoOrder);
        break;
      }
      case MsgType::CancelOrder: {
        auto* in = env.As<CancelOrder>();
        if (!in) return;
        auto it = by_client_.find(Key(in->client_id, in->client_order_id));
        if (it == by_client_.end()) return;   // unknown: a real gateway would reject
        // Cancels follow the same route as the original order so the owner
        // (OMS / algo) can translate them into ChildCancels.
        ChildCancel c;
        c.order_id = it->second;
        c.child_id = 0;
        c.owner = 0;
        Publish(BusId::Order, c, orders_[it->second].algo == AlgoType::None ? kSimpleOrder : kAlgoOrder);
        break;
      }
      case MsgType::ClientReport: {
        auto* rep = env.As<ClientReport>();
        if (!rep) return;
        ClientReport out = *rep;
        Publish(BusId::Client, out, kClientReport, uint16_t(out.client_id));
        if (IsTerminal(out.status)) {
          by_client_.erase(Key(out.client_id, out.client_order_id));
          orders_.erase(out.order_id);
        }
        break;
      }
      default: break;
    }
  }

  // Business-logic hook: pre-trade checks. Routing-only skeleton accepts everything.
  virtual bool ValidateOrder(const NewOrder&) { return true; }

  void Reject(const NewOrder& in)
  {
    ClientReport rep;
    rep.client_id = in.client_id;
    rep.client_order_id = in.client_order_id;
    rep.status = ExecStatus::Rejected;
    rep.leaves_qty = 0;
    Publish(BusId::Client, rep, kClientReport, uint16_t(in.client_id));
  }

private:
  struct Live { uint32_t client_id; uint64_t client_order_id; AlgoType algo; };
  static uint64_t Key(uint32_t client, uint64_t cl_ord) { return (uint64_t(client) << 48) ^ cl_ord; }
  std::unordered_map<uint64_t, Live> orders_;        // order_id -> client
  std::unordered_map<uint64_t, uint64_t> by_client_; // (client, cl_ord) -> order_id
};

} // namespace trading
