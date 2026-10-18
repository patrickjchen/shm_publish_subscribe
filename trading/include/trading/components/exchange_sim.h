#pragma once
// Exchange simulator: stands in for a venue gateway.
//   order_bus kExchangeOut (ExchangeOrder)  -> ExchangeReport New, then Filled  -> kExchangeIn
//   order_bus kExchangeOut (ExchangeCancel) -> ExchangeReport Canceled          -> kExchangeIn
// `fill_mode`: "full" (default) acks + fills immediately, "ack" only acks (orders rest
// until canceled), "partial" acks + fills half + fills the rest.
#include "trading/component.h"

#include <string>
#include <unordered_map>

namespace trading {

class ExchangeSim : public Component
{
public:
  ExchangeSim(const Buses& buses, std::string fill_mode = "full")
    : Component("exchange_sim", kSrcExchange, buses), fill_mode_(std::move(fill_mode))
  {
    Subscribe(BusId::Order, shmctx::TopicMask().set(kExchangeOut));
  }

protected:
  void OnMessage(const Envelope& env) override
  {
    switch (env.header().type) {
      case MsgType::ExchangeOrder: {
        auto* o = env.As<ExchangeOrder>();
        if (!o) return;
        Report(o->exch_order_id, ExecStatus::New, 0, 0, o->qty);
        if (fill_mode_ == "ack") { resting_[o->exch_order_id] = o->qty; return; }
        if (fill_mode_ == "partial") {
          double half = o->qty / 2;
          Report(o->exch_order_id, ExecStatus::PartiallyFilled, half, o->px, o->qty - half);
          Report(o->exch_order_id, ExecStatus::Filled, o->qty - half, o->px, 0);
          return;
        }
        Report(o->exch_order_id, ExecStatus::Filled, o->qty, o->px, 0);
        break;
      }
      case MsgType::ExchangeCancel: {
        auto* c = env.As<ExchangeCancel>();
        if (!c) return;
        auto it = resting_.find(c->exch_order_id);
        double leaves = it == resting_.end() ? 0 : it->second;
        if (it != resting_.end()) resting_.erase(it);
        Report(c->exch_order_id, ExecStatus::Canceled, 0, 0, leaves);
        break;
      }
      default: break;
    }
  }

private:
  void Report(uint64_t exch_order_id, ExecStatus st, double fill_qty, double fill_px, double leaves)
  {
    ExchangeReport r;
    r.exch_order_id = exch_order_id;
    r.exch_exec_id = NextId();
    r.status = st;
    r.fill_qty = fill_qty;
    r.fill_px = fill_px;
    r.leaves_qty = leaves;
    Publish(BusId::Order, r, kExchangeIn);
  }

  std::string fill_mode_;
  std::unordered_map<uint64_t, double> resting_;
};

} // namespace trading
