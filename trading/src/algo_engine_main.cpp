#include "main_common.h"
#include "trading/components/algo_engine.h"
int main(int argc, char** argv)
{
  return trading::RunMain(argc, argv, "algo engine: AlgoOrder -> N x EmsRequest; ExecReport(owner=ALGO) -> OwnerReport; consumes md_bus\n",
                          [](const trading::Buses& b, const trading::SystemConfig&, const trading::Args&) { return std::make_unique<trading::AlgoEngine>(b); });
}
