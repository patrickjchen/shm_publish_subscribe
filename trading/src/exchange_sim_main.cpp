#include "main_common.h"
#include "trading/components/exchange_sim.h"
int main(int argc, char** argv)
{
  return trading::RunMain(argc, argv, "exchange simulator: ExchangeOut -> ExchangeIn (ack + fill). [component exchange_sim] fill_mode = full|ack|partial\n",
                          [](const trading::Buses& b, const trading::SystemConfig& cfg, const trading::Args& a) {
                            auto it = a.extra.find("fill-mode");
                            std::string mode = it != a.extra.end() ? it->second : cfg.Opt("exchange_sim", "fill_mode", "full");
                            return std::make_unique<trading::ExchangeSim>(b, mode);
                          });
}
