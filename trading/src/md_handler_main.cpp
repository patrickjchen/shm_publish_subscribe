#include "main_common.h"
#include "trading/components/market_data_handler.h"
#include <sstream>
int main(int argc, char** argv)
{
  return trading::RunMain(argc, argv, "market data handler: publishes BookTicker/Trade on md_bus. [component md_handler] symbols, interval_us, sim\n",
                          [](const trading::Buses& b, const trading::SystemConfig& cfg, const trading::Args&) {
                            std::vector<std::string> symbols;
                            std::stringstream ss(cfg.Opt("md_handler", "symbols", "BTCUSDT,ETHUSDT"));
                            for (std::string s; std::getline(ss, s, ',');) if (!s.empty()) symbols.push_back(s);
                            auto interval = std::chrono::microseconds(std::stoll(cfg.Opt("md_handler", "interval_us", "1000")));
                            bool sim = cfg.Opt("md_handler", "sim", "true") != "false";
                            return std::make_unique<trading::MarketDataHandler>(b, symbols, interval, sim);
                          });
}
