// Remove the SysV segment for <key-file>. usage: shm_destroy <key-file>
#include "shmctx/shmctx.h"
#include <cstdio>

int main(int argc, char** argv)
{
  if (argc < 2) { std::fprintf(stderr, "usage: %s <key-file>\n", argv[0]); return 1; }
  mcp::core::ShmContext ctx(argv[1], true);
  int rc = ctx.Destroy();
  std::printf("destroy %s -> %d\n", argv[1], rc);
  return rc == 0 ? 0 : 1;
}
