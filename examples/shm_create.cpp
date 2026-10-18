// Create a SysV shared-memory queue. The segment outlives this process.
// usage: shm_create <key-file> [queue_size=1m] [consumers=8] [size_hint=64]
// Sizes accept k/m/g suffixes ("2g"). Total segment = storage + ledger (40 B per
// storage/size_hint slot) + 64 B per consumer, so prefer a larger size_hint for
// multi-GiB queues.
#include "demo_message.h"
#include "shmctx/byte_size.h"
#include "shmctx/shmctx.h"
#include <cstdio>
#include <cstdlib>

int main(int argc, char** argv)
{
  if (argc < 2) { std::fprintf(stderr, "usage: %s <key-file> [queue_size] [consumers] [size_hint]\n", argv[0]); return 1; }
  int64_t queue = argc > 2 ? shmctx::ParseByteSize(argv[2]) : (1 << 20);
  int64_t consumers = argc > 3 ? std::atoll(argv[3]) : 8;
  int64_t hint = argc > 4 ? shmctx::ParseByteSize(argv[4]) : 64;
  TouchKeyFile(argv[1]);
  mcp::core::ShmContext ctx(argv[1], true);
  if (ctx.Create(queue, consumers, hint) != 0) { std::fprintf(stderr, "Create failed\n"); return 1; }
  std::printf("created %s storage=%ld (%s) ledger=%ld consumers=%ld\n", argv[1], ctx.GetStorageSize(),
              shmctx::FormatByteSize(ctx.GetStorageSize()).c_str(), ctx.GetLedgerSize(), ctx.GetConsumers());
  return 0;
}
