#include "main_common.h"
#include "trading/components/order_entry_gateway.h"
int main(int argc, char** argv)
{
  return trading::RunMain(argc, argv, "order entry gateway: client_bus ClientOrder -> order_bus SimpleOrder|AlgoOrder; OwnerReport -> ClientReport\n",
                          [](const trading::Buses& b, const trading::SystemConfig&, const trading::Args&) {
                            return std::make_unique<trading::OrderEntryGateway>(b);
                          });
}
