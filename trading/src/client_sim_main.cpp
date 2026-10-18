// Client simulator: sends a mix of simple and algo orders through the gateway
// and waits until every one of them is terminal (or a timeout).
//   --orders N --algo-ratio 0.5 --client-id 7 --timeout-ms 10000 --cancel-ratio 0.0
#include "main_common.h"
#include "trading/components/client.h"

#include <chrono>
#include <thread>

int main(int argc, char** argv)
{
  using namespace trading;
  try {
    Args args = Args::Parse(argc, argv);
    SystemConfig cfg = SystemConfig::Load(args.config);
    auto opt = [&](const char* k, const char* def) { auto it = args.extra.find(k); return it != args.extra.end() ? it->second : cfg.Opt("client_sim", k, def); };
    uint32_t client_id = uint32_t(std::stoul(opt("client-id", "7")));
    uint32_t orders = uint32_t(std::stoul(opt("orders", "100")));
    double algo_ratio = std::stod(opt("algo-ratio", "0.5"));
    double cancel_ratio = std::stod(opt("cancel-ratio", "0"));
    int timeout_ms = std::stoi(opt("timeout-ms", "10000"));

    AttachedBuses attached(cfg);
    Client client(attached.buses, client_id);
    client.SetVerbose(args.verbose);
    std::printf("READY client_%u\n", client_id);
    std::fflush(stdout);

    auto t0 = std::chrono::steady_clock::now();
    const char* symbols[] = { "BTCUSDT", "ETHUSDT", "SOLUSDT" };
    uint32_t algo_sent = 0, cancels = 0;
    for (uint32_t i = 0; i < orders; ++i) {
      bool algo = (double(i % 100) / 100.0) < algo_ratio;
      auto id = client.SendOrder(symbols[i % 3], i % 2 ? Side::Buy : Side::Sell, 1.0 + i % 5, 100.0 + i % 50,
                                 algo ? AlgoType::Twap : AlgoType::None);
      algo_sent += algo;
      if (cancel_ratio > 0 && (double((i * 7) % 100) / 100.0) < cancel_ratio) { client.Cancel(id); ++cancels; }
      if (i % 64 == 0) client.Poll();
    }
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!client.AllDone() && std::chrono::steady_clock::now() < deadline) {
      if (!client.PollOnce()) std::this_thread::yield();
    }
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();

    uint32_t filled = 0, canceled = 0, rejected = 0, reports = 0;
    for (auto& [id, st] : client.Orders()) {
      filled += st.status == ExecStatus::Filled;
      canceled += st.status == ExecStatus::Canceled;
      rejected += st.status == ExecStatus::Rejected;
      reports += st.reports;
    }
    bool ok = client.AllDone();
    std::printf("client_%u: sent=%u (algo=%u cancels=%u) filled=%u canceled=%u rejected=%u open=%zu reports=%u elapsed_ms=%ld %s\n",
                client_id, orders, algo_sent, cancels, filled, canceled, rejected, client.Open(), reports, ms, ok ? "OK" : "TIMEOUT");
    std::printf("%s\n", client.Summary().c_str());
    return ok ? 0 : 2;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 64;
  }
}
