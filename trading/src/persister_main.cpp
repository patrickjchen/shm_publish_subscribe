#include "main_common.h"
#include "trading/components/persister.h"
int main(int argc, char** argv)
{
  return trading::RunMain(argc, argv, "persister: journals every client_bus/order_bus message. [component persister] out, log_md, flush_every\n",
                          [](const trading::Buses& b, const trading::SystemConfig& cfg, const trading::Args& a) {
                            auto it = a.extra.find("out");
                            std::string out = it != a.extra.end() ? it->second : cfg.Opt("persister", "out", "trading_journal.log");
                            bool log_md = cfg.Opt("persister", "log_md", "false") == "true";
                            uint32_t flush = uint32_t(std::stoul(cfg.Opt("persister", "flush_every", "256")));
                            return std::make_unique<trading::Persister>(b, out, log_md, flush);
                          });
}
