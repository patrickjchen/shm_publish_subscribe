// End-to-end routing over private (in-process) buses: every component runs in
// its own thread exactly as it would in its own process, the Client drives
// orders through the gateway and we assert on the per-topic counters and the
// persister journal. No SysV, no business logic.
#include "shm_test_support.h"
#include "trading/components/algo_engine.h"
#include "trading/components/client.h"
#include "trading/components/ems.h"
#include "trading/components/exchange_sim.h"
#include "trading/components/market_data_handler.h"
#include "trading/components/oms.h"
#include "trading/components/order_entry_gateway.h"
#include "trading/components/persister.h"

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <fstream>
#include <latch>
#include <thread>

using namespace trading;
namespace mc = mcp::core;

namespace {

struct System
{
  mc::ShmContext client_bus{ "client", false }, order_bus{ "order", false }, md_bus{ "md", false };
  Buses buses;
  std::string journal;
  std::unique_ptr<OrderEntryGateway> gateway;
  std::unique_ptr<Oms> oms;
  std::unique_ptr<AlgoEngine> algo;
  std::unique_ptr<Ems> ems;
  std::unique_ptr<ExchangeSim> exchange;
  std::unique_ptr<Persister> persister;
  std::unique_ptr<MarketDataHandler> md;
  std::vector<std::thread> threads;
  std::vector<Component*> running;

  explicit System(const std::string& fill_mode = "full", bool with_md = false)
  {
    REQUIRE(client_bus.Create(16 << 20, 8, 128) == 0);
    REQUIRE(order_bus.Create(64 << 20, 8, 128) == 0);
    REQUIRE(md_bus.Create(16 << 20, 8, 128) == 0);
    buses.client = &client_bus; buses.order = &order_bus; buses.md = &md_bus;
    journal = (std::filesystem::temp_directory_path() / ("trading_journal_" + std::to_string(::getpid()) + "_" + std::to_string(reinterpret_cast<uintptr_t>(this)) + ".log")).string();

    // Constructors subscribe, so every cursor exists before any order is sent.
    persister = std::make_unique<Persister>(buses, journal, /*log_md=*/false, /*flush_every=*/1);
    exchange = std::make_unique<ExchangeSim>(buses, fill_mode);
    ems = std::make_unique<Ems>(buses);
    oms = std::make_unique<Oms>(buses);
    algo = std::make_unique<AlgoEngine>(buses);
    gateway = std::make_unique<OrderEntryGateway>(buses);
    if (with_md) md = std::make_unique<MarketDataHandler>(buses, std::vector<std::string>{ "BTCUSDT", "ETHUSDT" }, std::chrono::microseconds(200));

    for (Component* c : std::initializer_list<Component*>{ persister.get(), exchange.get(), ems.get(), oms.get(), algo.get(), gateway.get(), md.get() })
      if (c) running.push_back(c);
    std::latch ready(std::ptrdiff_t(running.size()));
    for (Component* c : running) threads.emplace_back([c, &ready] { c->Run([&] { ready.count_down(); }); });
    ready.wait();
  }

  void Stop()
  {
    for (Component* c : running) c->RequestStop();
    for (auto& t : threads) t.join();
    threads.clear();
  }

  ~System()
  {
    if (!threads.empty()) Stop();
    std::filesystem::remove(journal);
  }

  std::vector<std::string> JournalLines() const
  {
    std::ifstream in(journal);
    std::vector<std::string> lines;
    for (std::string l; std::getline(in, l);) lines.push_back(l);
    return lines;
  }
  size_t CountJournal(const std::string& needle) const
  {
    size_t n = 0;
    for (auto& l : JournalLines()) if (l.find(needle) != std::string::npos) ++n;
    return n;
  }
};

bool Drive(Client& client, std::chrono::milliseconds timeout = std::chrono::seconds(10))
{
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!client.AllDone() && std::chrono::steady_clock::now() < deadline)
    if (!client.PollOnce()) std::this_thread::yield();
  return client.AllDone();
}
} // namespace

TEST_CASE("Simple order: client -> gateway -> OMS -> EMS -> exchange -> EMS -> OMS -> gateway -> client", "[trading][routing]")
{
  System sys;
  Client client(sys.buses, 7);
  auto id = client.SendOrder("BTCUSDT", Side::Buy, 2.0, 100.0);
  REQUIRE(Drive(client));
  sys.Stop();

  auto& st = client.Orders().at(id);
  CHECK(st.status == ExecStatus::Filled);
  CHECK(st.cum_qty == 2.0);
  CHECK(st.reports == 2);   // New (ack) + Filled

  CHECK(sys.gateway->InCount(kClientOrder) == 1);
  CHECK(sys.gateway->OutCount(kSimpleOrder) == 1);
  CHECK(sys.gateway->OutCount(kAlgoOrder) == 0);
  CHECK(sys.oms->InCount(kSimpleOrder) == 1);
  CHECK(sys.oms->OutCount(kEmsRequest) == 1);
  CHECK(sys.algo->InCount(kAlgoOrder) == 0);
  CHECK(sys.algo->InCount(kExecReport) == 0);          // owner filter: OMS reports never reach the algo engine
  CHECK(sys.ems->InCount(kEmsRequest) == 1);
  CHECK(sys.ems->OutCount(kExchangeOut) == 1);
  CHECK(sys.exchange->InCount(kExchangeOut) == 1);
  CHECK(sys.exchange->OutCount(kExchangeIn) == 2);
  CHECK(sys.ems->InCount(kExchangeIn) == 2);
  CHECK(sys.ems->OutCount(kExecReport) == 2);
  CHECK(sys.oms->InCount(kExecReport) == 2);
  CHECK(sys.oms->OutCount(kOwnerReport) == 2);
  CHECK(sys.gateway->InCount(kOwnerReport) == 2);
  CHECK(sys.gateway->OutCount(kClientReport) == 2);
  CHECK(sys.gateway->LiveOrders() == 0);
  CHECK(sys.oms->OpenOrders() == 0);
  CHECK(sys.ems->OpenExchangeOrders() == 0);
}

TEST_CASE("Algo order is routed to the algo engine and its reports come back by owner id", "[trading][routing]")
{
  System sys;
  Client client(sys.buses, 9);
  auto id = client.SendOrder("ETHUSDT", Side::Sell, 10.0, 2000.0, AlgoType::Twap);
  REQUIRE(Drive(client));
  sys.Stop();

  CHECK(client.Orders().at(id).status == ExecStatus::Filled);
  CHECK(sys.gateway->OutCount(kAlgoOrder) == 1);
  CHECK(sys.gateway->OutCount(kSimpleOrder) == 0);
  CHECK(sys.algo->InCount(kAlgoOrder) == 1);
  CHECK(sys.algo->OutCount(kEmsRequest) == 1);          // skeleton Slice(): one child
  CHECK(sys.algo->InCount(kExecReport) == 2);
  CHECK(sys.oms->InCount(kExecReport) == 0);            // owner filter the other way round
  CHECK(sys.oms->InCount(kSimpleOrder) == 0);
  CHECK(sys.algo->OutCount(kOwnerReport) == 2);
  CHECK(sys.algo->OpenParents() == 0);
}

TEST_CASE("Mixed flow from two clients: every message is journaled, reports reach the right client", "[trading][routing][persister]")
{
  System sys;
  Client a(sys.buses, 1), b(sys.buses, 2);
  constexpr int kPerClient = 200;
  for (int i = 0; i < kPerClient; ++i) {
    a.SendOrder("BTCUSDT", Side::Buy, 1, 100, i % 2 ? AlgoType::Twap : AlgoType::None);
    b.SendOrder("SOLUSDT", Side::Sell, 2, 50, i % 3 ? AlgoType::None : AlgoType::Vwap);
    if (i % 32 == 0) { a.Poll(); b.Poll(); }
  }
  REQUIRE(Drive(a));
  REQUIRE(Drive(b));
  // Let the persister catch the tail of the stream before stopping it.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  sys.Stop();

  for (auto* c : { &a, &b }) {
    CHECK(c->Orders().size() == kPerClient);
    for (auto& [id, st] : c->Orders()) { CHECK(st.status == ExecStatus::Filled); CHECK(st.reports == 2); }
    CHECK(c->InCount(kClientReport) == 2 * kPerClient);   // only its own reports (dest = client id)
  }
  const uint64_t orders = 2 * kPerClient;
  CHECK(sys.gateway->InCount(kClientOrder) == orders);
  CHECK(sys.gateway->OutCount(kSimpleOrder) + sys.gateway->OutCount(kAlgoOrder) == orders);
  CHECK(sys.gateway->OutCount(kAlgoOrder) == 100 + 67);   // a: odd i (100); b: i % 3 == 0 (67)
  CHECK(sys.ems->InCount(kEmsRequest) == orders);
  CHECK(sys.exchange->OutCount(kExchangeIn) == 2 * orders);
  CHECK(sys.ems->OutCount(kExecReport) == 2 * orders);
  CHECK(sys.oms->InCount(kExecReport) + sys.algo->InCount(kExecReport) == 2 * orders);
  CHECK(sys.gateway->OutCount(kClientReport) == 2 * orders);

  // Persister saw every message on both buses.
  const uint64_t client_bus_msgs = orders /*NewOrder*/ + 2 * orders /*ClientReport*/;
  const uint64_t order_bus_msgs = orders /*OrderRequest*/ + orders /*ChildOrder*/ + orders /*ExchangeOrder*/ + 2 * orders /*ExchangeReport*/
                                  + 2 * orders /*ExecReport*/ + 2 * orders /*OwnerReport*/;
  CHECK(sys.persister->Lines() == client_bus_msgs + order_bus_msgs);
  CHECK(sys.persister->InCount(kClientOrder) == orders);
  CHECK(sys.persister->InCount(kExchangeIn) == 2 * orders);
  auto lines = sys.JournalLines();
  CHECK(lines.size() == client_bus_msgs + order_bus_msgs);
  CHECK(sys.CountJournal("topic=ExchangeIn") == 2 * orders);
  CHECK(sys.CountJournal("ExecReport") == 2 * orders);
  CHECK(sys.CountJournal("topic=ClientReport dest=1 ") == 2 * kPerClient);
  CHECK(sys.CountJournal("topic=ClientReport dest=2 ") == 2 * kPerClient);
  CHECK(sys.CountJournal("Filled") >= 4 * orders);     // exchange report + exec report + owner + client report per order
  // Lines are self-describing: bus, seq, ts, topic, dest, src, type, ids.
  REQUIRE(!lines.empty());
  CHECK(lines[0].rfind("client_bus seq=", 0) == 0);
  CHECK(lines[0].find("topic=ClientOrder") != std::string::npos);
  CHECK(lines[0].find("NewOrder client=") != std::string::npos);
}

TEST_CASE("Cancel follows the order's route and comes back Canceled", "[trading][routing][cancel]")
{
  System sys("ack");    // exchange only acks: orders rest until canceled
  Client client(sys.buses, 3);
  auto simple = client.SendOrder("BTCUSDT", Side::Buy, 1, 100);
  auto algo = client.SendOrder("BTCUSDT", Side::Buy, 1, 100, AlgoType::Pov);
  // Wait for both acks so the owners know their children.
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while ((client.Orders().at(simple).reports < 1 || client.Orders().at(algo).reports < 1) && std::chrono::steady_clock::now() < deadline)
    client.PollOnce();
  REQUIRE(client.Orders().at(simple).status == ExecStatus::New);
  client.Cancel(simple);
  client.Cancel(algo);
  REQUIRE(Drive(client));
  sys.Stop();

  CHECK(client.Orders().at(simple).status == ExecStatus::Canceled);
  CHECK(client.Orders().at(algo).status == ExecStatus::Canceled);
  CHECK(sys.gateway->InCount(kClientOrder) == 4);        // 2 NewOrder + 2 CancelOrder
  CHECK(sys.oms->OutCount(kEmsRequest) == 2);           // ChildOrder + ChildCancel
  CHECK(sys.algo->OutCount(kEmsRequest) == 2);
  CHECK(sys.ems->OutCount(kExchangeOut) == 4);          // 2 orders + 2 cancels
  CHECK(sys.exchange->OutCount(kExchangeIn) == 4);      // 2 acks + 2 canceled
  CHECK(sys.ems->OpenExchangeOrders() == 0);
  CHECK(sys.persister->InCount(kClientOrder) == 4);
}

TEST_CASE("Partial fills aggregate per parent; market data reaches OMS and algo engine only", "[trading][routing][md]")
{
  System sys("partial", /*with_md=*/true);
  Client client(sys.buses, 5);
  auto id = client.SendOrder("ETHUSDT", Side::Buy, 4.0, 10.0, AlgoType::Twap);
  REQUIRE(Drive(client));
  // Give the simulated feed a few ticks.
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  sys.Stop();

  auto& st = client.Orders().at(id);
  CHECK(st.status == ExecStatus::Filled);
  CHECK(st.cum_qty == 4.0);
  CHECK(st.reports == 3);                                // New, PartiallyFilled, Filled
  CHECK(sys.exchange->OutCount(kExchangeIn) == 3);

  CHECK(sys.md->TicksPublished() > 0);
  CHECK(sys.oms->MarketDataSeen() > 0);
  CHECK(sys.algo->MarketDataSeen() > 0);
  CHECK(sys.oms->InCount(kBookTicker) == sys.oms->InCount(kTrade));
  CHECK(sys.ems->InCount(kBookTicker) == 0);             // EMS / gateway / persister do not subscribe to md_bus
  CHECK(sys.gateway->InCount(kBookTicker) == 0);
  CHECK(sys.persister->InCount(kBookTicker) == 0);       // log_md = false
}

TEST_CASE("Component idle timeout stops a quiet component", "[trading][component]")
{
  mc::ShmContext c{ "c", false }, o{ "o", false };
  REQUIRE(c.Create(1 << 20, 2, 64) == 0);
  REQUIRE(o.Create(1 << 20, 2, 64) == 0);
  Buses b; b.client = &c; b.order = &o;   // no md_bus: OMS must cope
  Oms oms(b);
  oms.SetIdleTimeout(std::chrono::milliseconds(100));
  auto t0 = std::chrono::steady_clock::now();
  oms.Run();
  CHECK(oms.IdleTimedOut());
  CHECK(std::chrono::steady_clock::now() - t0 >= std::chrono::milliseconds(100));
  CHECK(oms.InTotal() == 0);
  CHECK(oms.Summary().rfind("oms: in=0 out=0", 0) == 0);
}

TEST_CASE("Wire messages are PODs with the bus header first", "[trading][messages]")
{
  static_assert(offsetof(Header, seq) == kSeqOffset);
  static_assert(offsetof(Header, ts) == kTsOffset);
  NewOrder o;
  CHECK(o.h.type == MsgType::NewOrder);
  CHECK(o.h.len == sizeof(NewOrder));
  ExecReport er; er.order_id = 42; er.owner = kOwnerAlgo; er.status = ExecStatus::PartiallyFilled; er.fill_qty = 1.5; er.fill_px = 99.5;
  auto s = Describe(BusId::Order, kExecReport, kOwnerAlgo, reinterpret_cast<const char*>(&er), sizeof(er));
  CHECK(s.find("order_bus") == 0);
  CHECK(s.find("topic=ExecReport dest=2") != std::string::npos);
  CHECK(s.find("order=42") != std::string::npos);
  CHECK(s.find("PartiallyFilled fill=1.5000@99.5000") != std::string::npos);
  CHECK(std::string(Describe(BusId::Md, kTrade, 0, "xx", 2)).find("no header") != std::string::npos);
}
