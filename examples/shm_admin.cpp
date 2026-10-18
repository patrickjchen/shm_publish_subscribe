// Print queue header state. usage: shm_admin <key-file>
#include "shmctx/shmctx.h"
#include <cstdio>

int main(int argc, char** argv)
{
  if (argc < 2) { std::fprintf(stderr, "usage: %s <key-file>\n", argv[0]); return 1; }
  mcp::core::ShmContext ctx(argv[1], true);
  if (ctx.Attach() != 0) { std::fprintf(stderr, "Attach failed\n"); return 1; }
  mcp::core::ShmAdmin admin(ctx);
  std::printf("storage=%ld ledger=%ld consumers=%ld\n", ctx.GetStorageSize(), ctx.GetLedgerSize(), ctx.GetConsumers());
  std::printf("committed_seq=%ld claimed_seq=%ld in_flight=%ld\n",
              admin.GetCurrentSequenceNumber(), admin.GetClaimedSequenceNumber(),
              admin.GetClaimedSequenceNumber() - admin.GetCurrentSequenceNumber());
  std::vector<mcp::core::ConsumerStats> stats;
  admin.GetConsumerStats(stats);
  for (size_t i = 0; i < stats.size(); ++i)
    std::printf("consumer[%zu]: occupied=%d pid=%d seq=%ld\n", i, stats[i].occupied, stats[i].consumer_pid, stats[i].sequence_num);
  return 0;
}
