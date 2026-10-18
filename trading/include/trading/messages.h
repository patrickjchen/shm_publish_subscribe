#pragma once
// Wire messages for the algo-trading skeleton. All PODs, fixed size, 8-byte
// aligned. `Header` comes first in every message: PacketProducer stamps the
// bus sequence number at offset 0 and the publish timestamp at offset 8.
//
// Topics identify the *kind* of traffic on a bus; the SHM `strategy_id` field
// is used as a *destination/owner id* so that one consumer can filter
// "execution reports for me" without a topic per component.

#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

namespace trading {

/////////////////////////////////////////////////////////////////////////////
// Buses, topics, ids
/////////////////////////////////////////////////////////////////////////////

enum class BusId : uint8_t { Client = 0, Order = 1, Md = 2 };
inline const char* ToString(BusId b) { return b == BusId::Client ? "client_bus" : b == BusId::Order ? "order_bus" : "md_bus"; }

// Topic numbers must stay < 64 (SubscribedTopics is a bitset<64>).
enum Topic : uint16_t {
  // client_bus
  kClientOrder = 1,    // client            -> gateway        NewOrder / CancelOrder
  kClientReport = 2,   // gateway           -> client         ClientReport   (strategy_id = client_id)
  // order_bus
  kSimpleOrder = 3,    // gateway           -> OMS            OrderRequest
  kAlgoOrder = 4,      // gateway           -> algo engine    OrderRequest
  kEmsRequest = 5,     // OMS / algo engine -> EMS            ChildOrder / ChildCancel
  kExchangeOut = 6,    // EMS               -> exchange       ExchangeOrder
  kExchangeIn = 7,     // exchange          -> EMS            ExchangeReport (ack / fill / cancel)
  kExecReport = 8,     // EMS               -> OMS / algo     ExecReport     (strategy_id = owner)
  kOwnerReport = 9,    // OMS / algo        -> gateway        ClientReport
  // md_bus
  kBookTicker = 16,    // market data handler -> OMS / algo
  kTrade = 17,
};

inline const char* ToString(Topic t)
{
  switch (t) {
    case kClientOrder: return "ClientOrder";
    case kClientReport: return "ClientReport";
    case kSimpleOrder: return "SimpleOrder";
    case kAlgoOrder: return "AlgoOrder";
    case kEmsRequest: return "EmsRequest";
    case kExchangeOut: return "ExchangeOut";
    case kExchangeIn: return "ExchangeIn";
    case kExecReport: return "ExecReport";
    case kOwnerReport: return "OwnerReport";
    case kBookTicker: return "BookTicker";
    case kTrade: return "Trade";
  }
  return "?";
}

// Owner of a child order = who receives its execution reports.
enum Owner : uint16_t { kOwnerOms = 1, kOwnerAlgo = 2 };

// Component source ids (Header::src).
enum Source : uint16_t {
  kSrcGateway = 10, kSrcOms = 20, kSrcAlgo = 30, kSrcEms = 40, kSrcExchange = 50, kSrcMdHandler = 60, kSrcPersister = 70,
  kSrcClientBase = 100,   // + client_id
};

enum class MsgType : uint16_t {
  NewOrder = 1, CancelOrder, OrderRequest, ChildOrder, ChildCancel, ExchangeOrder, ExchangeCancel, ExchangeReport,
  ExecReport, ClientReport, BookTicker, Trade,
};

inline const char* ToString(MsgType t)
{
  switch (t) {
    case MsgType::NewOrder: return "NewOrder";
    case MsgType::CancelOrder: return "CancelOrder";
    case MsgType::OrderRequest: return "OrderRequest";
    case MsgType::ChildOrder: return "ChildOrder";
    case MsgType::ChildCancel: return "ChildCancel";
    case MsgType::ExchangeOrder: return "ExchangeOrder";
    case MsgType::ExchangeCancel: return "ExchangeCancel";
    case MsgType::ExchangeReport: return "ExchangeReport";
    case MsgType::ExecReport: return "ExecReport";
    case MsgType::ClientReport: return "ClientReport";
    case MsgType::BookTicker: return "BookTicker";
    case MsgType::Trade: return "Trade";
  }
  return "?";
}

enum class Side : uint8_t { Buy = 1, Sell = 2 };
enum class AlgoType : uint8_t { None = 0, Twap = 1, Vwap = 2, Pov = 3 };
enum class ExecStatus : uint8_t { New = 1, PartiallyFilled = 2, Filled = 3, Canceled = 4, Rejected = 5 };

inline const char* ToString(Side s) { return s == Side::Buy ? "Buy" : "Sell"; }
inline const char* ToString(AlgoType a)
{
  switch (a) { case AlgoType::None: return "None"; case AlgoType::Twap: return "TWAP"; case AlgoType::Vwap: return "VWAP"; case AlgoType::Pov: return "POV"; }
  return "?";
}
inline const char* ToString(ExecStatus s)
{
  switch (s) {
    case ExecStatus::New: return "New"; case ExecStatus::PartiallyFilled: return "PartiallyFilled"; case ExecStatus::Filled: return "Filled";
    case ExecStatus::Canceled: return "Canceled"; case ExecStatus::Rejected: return "Rejected";
  }
  return "?";
}
inline bool IsTerminal(ExecStatus s) { return s == ExecStatus::Filled || s == ExecStatus::Canceled || s == ExecStatus::Rejected; }

/////////////////////////////////////////////////////////////////////////////
// Common header + helpers
/////////////////////////////////////////////////////////////////////////////

struct Header
{
  uint64_t seq = 0;       // stamped by the bus (PacketProducer) at publish
  int64_t ts = 0;         // stamped by the bus at publish (ns since epoch)
  MsgType type{};
  uint16_t src = 0;       // Source id of the publisher
  uint32_t len = 0;       // sizeof(message)
};
static_assert(sizeof(Header) == 24);
constexpr size_t kSeqOffset = 0;
constexpr size_t kTsOffset = 8;

struct Symbol
{
  char v[16] = {};
  Symbol() = default;
  explicit Symbol(const char* s) { std::strncpy(v, s, sizeof(v) - 1); }
  std::string str() const { return std::string(v, strnlen(v, sizeof(v))); }
  bool operator==(const Symbol& o) const { return std::strncmp(v, o.v, sizeof(v)) == 0; }
};

/////////////////////////////////////////////////////////////////////////////
// client_bus
/////////////////////////////////////////////////////////////////////////////

struct NewOrder          // client -> gateway
{
  Header h{ 0, 0, MsgType::NewOrder, 0, sizeof(NewOrder) };
  uint32_t client_id = 0;
  uint32_t pad0 = 0;
  uint64_t client_order_id = 0;
  Symbol symbol;
  Side side{};
  AlgoType algo{};        // None -> OMS, anything else -> algo engine
  uint8_t pad1[6] = {};
  double qty = 0, px = 0;
  int64_t algo_duration_ns = 0;   // opaque algo parameter, passed through
};

struct CancelOrder       // client -> gateway (routing only; EMS stub acks it as Canceled)
{
  Header h{ 0, 0, MsgType::CancelOrder, 0, sizeof(CancelOrder) };
  uint32_t client_id = 0;
  uint32_t pad0 = 0;
  uint64_t client_order_id = 0;
};

struct ClientReport      // OMS/algo -> gateway (kOwnerReport) -> client (kClientReport)
{
  Header h{ 0, 0, MsgType::ClientReport, 0, sizeof(ClientReport) };
  uint32_t client_id = 0;
  ExecStatus status{};
  uint8_t pad0[3] = {};
  uint64_t client_order_id = 0;
  uint64_t order_id = 0;          // gateway-assigned
  double fill_qty = 0, fill_px = 0, cum_qty = 0, leaves_qty = 0;
};

/////////////////////////////////////////////////////////////////////////////
// order_bus
/////////////////////////////////////////////////////////////////////////////

struct OrderRequest      // gateway -> OMS (kSimpleOrder) | algo engine (kAlgoOrder)
{
  Header h{ 0, 0, MsgType::OrderRequest, 0, sizeof(OrderRequest) };
  uint64_t order_id = 0;
  uint64_t client_order_id = 0;
  uint32_t client_id = 0;
  Side side{};
  AlgoType algo{};
  uint8_t pad0[2] = {};
  Symbol symbol;
  double qty = 0, px = 0;
  int64_t algo_duration_ns = 0;
};

struct ChildOrder        // OMS/algo -> EMS (kEmsRequest)
{
  Header h{ 0, 0, MsgType::ChildOrder, 0, sizeof(ChildOrder) };
  uint64_t order_id = 0;          // parent (gateway) order
  uint64_t child_id = 0;          // owner-assigned
  uint32_t client_id = 0;
  uint16_t owner = 0;             // Owner: who gets the ExecReports
  Side side{};
  uint8_t pad0 = 0;
  Symbol symbol;
  double qty = 0, px = 0;
};

struct ChildCancel       // OMS/algo -> EMS (kEmsRequest)
{
  Header h{ 0, 0, MsgType::ChildCancel, 0, sizeof(ChildCancel) };
  uint64_t order_id = 0;
  uint64_t child_id = 0;
  uint16_t owner = 0;
  uint8_t pad0[6] = {};
};

struct ExchangeOrder     // EMS -> exchange (kExchangeOut)
{
  Header h{ 0, 0, MsgType::ExchangeOrder, 0, sizeof(ExchangeOrder) };
  uint64_t exch_order_id = 0;     // EMS-assigned client order id at the exchange
  Symbol symbol;
  Side side{};
  uint8_t pad0[7] = {};
  double qty = 0, px = 0;
};

struct ExchangeCancel    // EMS -> exchange (kExchangeOut)
{
  Header h{ 0, 0, MsgType::ExchangeCancel, 0, sizeof(ExchangeCancel) };
  uint64_t exch_order_id = 0;
};

struct ExchangeReport    // exchange -> EMS (kExchangeIn): ack, fill, cancel, reject
{
  Header h{ 0, 0, MsgType::ExchangeReport, 0, sizeof(ExchangeReport) };
  uint64_t exch_order_id = 0;
  uint64_t exch_exec_id = 0;
  ExecStatus status{};
  uint8_t pad0[7] = {};
  double fill_qty = 0, fill_px = 0, leaves_qty = 0;
};

struct ExecReport        // EMS -> OMS/algo (kExecReport, strategy_id = owner)
{
  Header h{ 0, 0, MsgType::ExecReport, 0, sizeof(ExecReport) };
  uint64_t order_id = 0;
  uint64_t child_id = 0;
  uint64_t exch_order_id = 0;
  uint64_t exch_exec_id = 0;
  uint32_t client_id = 0;
  uint16_t owner = 0;
  ExecStatus status{};
  uint8_t pad0 = 0;
  double fill_qty = 0, fill_px = 0, cum_qty = 0, leaves_qty = 0;
};

/////////////////////////////////////////////////////////////////////////////
// md_bus
/////////////////////////////////////////////////////////////////////////////

struct BookTicker
{
  Header h{ 0, 0, MsgType::BookTicker, 0, sizeof(BookTicker) };
  Symbol symbol;
  double bid_px = 0, bid_qty = 0, ask_px = 0, ask_qty = 0;
  int64_t exch_ts = 0;
};

struct Trade
{
  Header h{ 0, 0, MsgType::Trade, 0, sizeof(Trade) };
  Symbol symbol;
  double px = 0, qty = 0;
  Side aggressor{};
  uint8_t pad0[7] = {};
  int64_t exch_ts = 0;
};

#define TRADING_CHECK_POD(T) static_assert(std::is_trivially_copyable_v<T> && sizeof(T) % 8 == 0 && offsetof(T, h) == 0)
TRADING_CHECK_POD(NewOrder); TRADING_CHECK_POD(CancelOrder); TRADING_CHECK_POD(ClientReport); TRADING_CHECK_POD(OrderRequest);
TRADING_CHECK_POD(ChildOrder); TRADING_CHECK_POD(ChildCancel); TRADING_CHECK_POD(ExchangeOrder); TRADING_CHECK_POD(ExchangeCancel);
TRADING_CHECK_POD(ExchangeReport); TRADING_CHECK_POD(ExecReport); TRADING_CHECK_POD(BookTicker); TRADING_CHECK_POD(Trade);
#undef TRADING_CHECK_POD

/////////////////////////////////////////////////////////////////////////////
// Decoding helpers
/////////////////////////////////////////////////////////////////////////////

inline const Header* PeekHeader(const char* data, int64_t size)
{
  return size >= int64_t(sizeof(Header)) ? reinterpret_cast<const Header*>(data) : nullptr;
}

template<typename T>
inline const T* As(const char* data, int64_t size)
{
  return size >= int64_t(sizeof(T)) ? reinterpret_cast<const T*>(data) : nullptr;
}

// One-line, human-readable rendering used by the persister and verbose logs.
inline std::string Describe(BusId bus, uint16_t topic, uint16_t strategy_id, const char* data, int64_t size)
{
  char buf[512];
  const Header* h = PeekHeader(data, size);
  if (!h) { std::snprintf(buf, sizeof(buf), "%s topic=%u dest=%u <%ld bytes, no header>", ToString(bus), topic, strategy_id, size); return buf; }
  int n = std::snprintf(buf, sizeof(buf), "%s seq=%llu ts=%lld topic=%s dest=%u src=%u %s", ToString(bus), (unsigned long long)h->seq,
                        (long long)h->ts, ToString(Topic(topic)), strategy_id, h->src, ToString(h->type));
  auto rest = [&](const char* fmt, auto... args) { std::snprintf(buf + n, sizeof(buf) - size_t(n), fmt, args...); };
  switch (h->type) {
    case MsgType::NewOrder: {
      if (auto* m = As<NewOrder>(data, size))
        rest(" client=%u cl_ord=%llu %s %s %s qty=%.4f px=%.4f", m->client_id, (unsigned long long)m->client_order_id, m->symbol.str().c_str(), ToString(m->side), ToString(m->algo), m->qty, m->px);
      break;
    }
    case MsgType::CancelOrder: {
      if (auto* m = As<CancelOrder>(data, size))
        rest(" client=%u cl_ord=%llu", m->client_id, (unsigned long long)m->client_order_id);
      break;
    }
    case MsgType::OrderRequest: {
      if (auto* m = As<OrderRequest>(data, size))
        rest(" order=%llu client=%u cl_ord=%llu %s %s %s qty=%.4f px=%.4f", (unsigned long long)m->order_id, m->client_id, (unsigned long long)m->client_order_id, m->symbol.str().c_str(), ToString(m->side), ToString(m->algo), m->qty, m->px);
      break;
    }
    case MsgType::ChildOrder: {
      if (auto* m = As<ChildOrder>(data, size))
        rest(" order=%llu child=%llu owner=%u client=%u %s %s qty=%.4f px=%.4f", (unsigned long long)m->order_id, (unsigned long long)m->child_id, m->owner, m->client_id, m->symbol.str().c_str(), ToString(m->side), m->qty, m->px);
      break;
    }
    case MsgType::ChildCancel: {
      if (auto* m = As<ChildCancel>(data, size))
        rest(" order=%llu child=%llu owner=%u", (unsigned long long)m->order_id, (unsigned long long)m->child_id, m->owner);
      break;
    }
    case MsgType::ExchangeOrder: {
      if (auto* m = As<ExchangeOrder>(data, size))
        rest(" exch_ord=%llu %s %s qty=%.4f px=%.4f", (unsigned long long)m->exch_order_id, m->symbol.str().c_str(), ToString(m->side), m->qty, m->px);
      break;
    }
    case MsgType::ExchangeCancel: {
      if (auto* m = As<ExchangeCancel>(data, size))
        rest(" exch_ord=%llu", (unsigned long long)m->exch_order_id);
      break;
    }
    case MsgType::ExchangeReport: {
      if (auto* m = As<ExchangeReport>(data, size))
        rest(" exch_ord=%llu exec=%llu %s fill=%.4f@%.4f leaves=%.4f", (unsigned long long)m->exch_order_id, (unsigned long long)m->exch_exec_id, ToString(m->status), m->fill_qty, m->fill_px, m->leaves_qty);
      break;
    }
    case MsgType::ExecReport: {
      if (auto* m = As<ExecReport>(data, size))
        rest(" order=%llu child=%llu exch_ord=%llu owner=%u client=%u %s fill=%.4f@%.4f cum=%.4f leaves=%.4f", (unsigned long long)m->order_id, (unsigned long long)m->child_id, (unsigned long long)m->exch_order_id, m->owner, m->client_id, ToString(m->status), m->fill_qty, m->fill_px, m->cum_qty, m->leaves_qty);
      break;
    }
    case MsgType::ClientReport: {
      if (auto* m = As<ClientReport>(data, size))
        rest(" client=%u cl_ord=%llu order=%llu %s fill=%.4f@%.4f cum=%.4f leaves=%.4f", m->client_id, (unsigned long long)m->client_order_id, (unsigned long long)m->order_id, ToString(m->status), m->fill_qty, m->fill_px, m->cum_qty, m->leaves_qty);
      break;
    }
    case MsgType::BookTicker: {
      if (auto* m = As<BookTicker>(data, size))
        rest(" %s bid=%.4f x %.4f ask=%.4f x %.4f", m->symbol.str().c_str(), m->bid_px, m->bid_qty, m->ask_px, m->ask_qty);
      break;
    }
    case MsgType::Trade: {
      if (auto* m = As<Trade>(data, size))
        rest(" %s px=%.4f qty=%.4f %s", m->symbol.str().c_str(), m->px, m->qty, ToString(m->aggressor));
      break;
    }
  }
  return buf;
}

} // namespace trading
