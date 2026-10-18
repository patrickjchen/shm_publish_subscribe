#include "main_common.h"
#include "trading/components/ems.h"
int main(int argc, char** argv)
{
  return trading::RunMain(argc, argv, "EMS: EmsRequest -> ExchangeOut; ExchangeIn -> ExecReport(dest=owner)\n",
                          [](const trading::Buses& b, const trading::SystemConfig&, const trading::Args&) { return std::make_unique<trading::Ems>(b); });
}
