#!/usr/bin/env bash
# Re-vendor the SHM headers from the monorepo and re-apply the two local edits
# (include paths rooted at shmctx/, and the guarded test-hook friend decl).
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${1:-$HERE/../../cpp/core-libraries/include}"
DST="$HERE/include/shmctx"
for f in shm_context.h shm_queue_types.h spin_lock.h macros.h; do cp "$SRC/$f" "$DST/$f"; done
sed -i 's|#include "shm_queue_types.h"|#include "shmctx/shm_queue_types.h"|; s|#include "spin_lock.h"|#include "shmctx/spin_lock.h"|' "$DST/shm_context.h"
sed -i 's|#include "macros.h"|#include "shmctx/macros.h"|' "$DST/spin_lock.h"
python3 - "$DST/shm_context.h" <<'PY'
import sys; p=sys.argv[1]; s=open(p).read()
old="  friend class ShmAdmin;\n};"
new="  friend class ShmAdmin;\n#ifdef SHMCTX_ENABLE_TEST_HOOKS\n  friend struct ShmContextTestHooks;\n#endif\n};"
assert s.count(old)==1; open(p,'w').write(s.replace(old,new))
PY
echo "synced from $SRC"
