#include "main_common.h"
#include "trading/components/oms.h"
int main(int argc, char** argv)
{
  return trading::RunMain(argc, argv, "OMS: SimpleOrder -> EmsRequest; ExecReport(owner=OMS) -> OwnerReport; consumes md_bus\n",
                          [](const trading::Buses& b, const trading::SystemConfig&, const trading::Args&) { return std::make_unique<trading::Oms>(b); });
}
