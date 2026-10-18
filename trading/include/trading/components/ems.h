#pragma once
// EMS: the only component talking to exchanges.
//   order_bus kEmsRequest (ChildOrder)  -> ExchangeOrder  -> order_bus kExchangeOut
//   order_bus kEmsRequest (ChildCancel) -> ExchangeCancel -> order_bus kExchangeOut
//   order_bus kExchangeIn (ExchangeReport) -> ExecReport -> order_bus kExecReport, dest = owner
// Hooks: venue selection / smart order routing would go in ChooseVenue().
#include "trading/component.h"

#include <unordered_map>

namespace trading {

class Ems : public Component
{
public:
  explicit Ems(const Buses& buses) : Component("ems", kSrcEms, buses)
  {
    Subscribe(BusId::Order, shmctx::TopicMask().set(kEmsRequest).set(kExchangeIn));
  }

  size_t OpenExchangeOrders() const { return live_.size(); }

protected:
  void OnMessage(const Envelope& env) override
  {
    switch (env.header().type) {
      case MsgType::ChildOrder: {
        auto* child = env.As<ChildOrder>();
        if (!child) return;
        ExchangeOrder out;
        out.exch_order_id = NextId();
        out.symbol = child->symbol;
        out.side = child->side;
        out.qty = child->qty;
        out.px = child->px;
        live_[out.exch_order_id] = { *child, 0.0 };
        by_child_[child->child_id] = out.exch_order_id;
        Publish(BusId::Order, out, kExchangeOut);
        break;
      }
      case MsgType::ChildCancel: {
        auto* c = env.As<ChildCancel>();
        if (!c) return;
        auto it = by_child_.find(c->child_id);
        if (it == by_child_.end()) return;
        ExchangeCancel out;
        out.exch_order_id = it->second;
        Publish(BusId::Order, out, kExchangeOut);
        break;
      }
      case MsgType::ExchangeReport: {
        auto* rep = env.As<ExchangeReport>();
        if (!rep) return;
        auto it = live_.find(rep->exch_order_id);
        if (it == live_.end()) return;   // unknown exchange order: a real EMS would alert
        Live& l = it->second;
        l.cum_qty += rep->fill_qty;
        ExecReport er;
        er.order_id = l.child.order_id;
        er.child_id = l.child.child_id;
        er.exch_order_id = rep->exch_order_id;
        er.exch_exec_id = rep->exch_exec_id;
        er.client_id = l.child.client_id;
        er.owner = l.child.owner;
        er.status = rep->status;
        er.fill_qty = rep->fill_qty;
        er.fill_px = rep->fill_px;
        er.cum_qty = l.cum_qty;
        er.leaves_qty = rep->leaves_qty;
        Publish(BusId::Order, er, kExecReport, l.child.owner);   // routed to OMS or algo engine
        if (IsTerminal(rep->status)) {
          by_child_.erase(l.child.child_id);
          live_.erase(it);
        }
        break;
      }
      default: break;
    }
  }

private:
  struct Live { ChildOrder child; double cum_qty; };
  std::unordered_map<uint64_t, Live> live_;        // exch_order_id -> child
  std::unordered_map<uint64_t, uint64_t> by_child_; // child_id -> exch_order_id
};

} // namespace trading
