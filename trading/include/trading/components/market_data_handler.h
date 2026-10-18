#pragma once
// Market data handler: normalizes venue feeds onto md_bus.
//   (venue feed)  -> OnRawFeed()  -> md_bus kBookTicker / kTrade
// Routing only: in `sim` mode OnIdle() synthesizes a ticker + trade per symbol
// every `interval`. A real handler would decode a multicast/WebSocket feed in
// OnRawFeed() and publish the same two message types.
#include "trading/component.h"

#include <string>
#include <vector>

namespace trading {

class MarketDataHandler : public Component
{
public:
  MarketDataHandler(const Buses& buses, std::vector<std::string> symbols, std::chrono::microseconds interval, bool sim = true)
    : Component("md_handler", kSrcMdHandler, buses), interval_(interval), sim_(sim)
  {
    for (auto& s : symbols) symbols_.push_back(Symbol(s.c_str()));
  }

  uint64_t TicksPublished() const { return ticks_; }

protected:
  void OnMessage(const Envelope&) override {}   // subscribes to nothing in the skeleton

  void OnIdle() override
  {
    if (!sim_) return;
    auto now = std::chrono::steady_clock::now();
    if (now - last_tick_ < interval_) return;
    last_tick_ = now;
    for (size_t i = 0; i < symbols_.size(); ++i) {
      double mid = 100.0 * double(i + 1) + double(ticks_ % 100) * 0.01;
      BookTicker bt;
      bt.symbol = symbols_[i];
      bt.bid_px = mid - 0.01; bt.bid_qty = 5; bt.ask_px = mid + 0.01; bt.ask_qty = 7;
      bt.exch_ts = NowNs();
      Publish(BusId::Md, bt, kBookTicker);
      Trade tr;
      tr.symbol = symbols_[i];
      tr.px = mid; tr.qty = 0.5; tr.aggressor = (ticks_ & 1) ? Side::Buy : Side::Sell;
      tr.exch_ts = bt.exch_ts;
      Publish(BusId::Md, tr, kTrade);
      ++ticks_;
    }
  }

  // Business-logic hook for a real feed decoder.
  virtual void OnRawFeed(const char*, size_t) {}

private:
  std::vector<Symbol> symbols_;
  std::chrono::microseconds interval_;
  bool sim_;
  std::chrono::steady_clock::time_point last_tick_{};
  uint64_t ticks_ = 0;
};

} // namespace trading
