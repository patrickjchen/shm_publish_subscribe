#!/usr/bin/env bash
# End-to-end: create segment -> start consumer -> 3 producers in parallel ->
# verify consumer saw every message intact and ordered -> destroy.
# usage: cli_pipeline.sh <dir-with-example-binaries>
set -euo pipefail
BIN="${1:?bin dir}"
KEY="$(mktemp "${SHMCTX_TEST_TMPDIR:-${TMPDIR:-/tmp}}/shmctx_cli_XXXXXX")"
LOG="$(mktemp)"
PER=100000; PRODUCERS=3
QUEUE="${SHMCTX_CLI_QUEUE_SIZE:-1g}"   # 1 GiB storage; size_hint 512 keeps the ledger at 80 MiB
cleanup() { "$BIN/shm_destroy" "$KEY" >/dev/null 2>&1 || true; rm -f "$KEY" "$LOG"; }
trap cleanup EXIT

# Skip (ctest SKIP_RETURN_CODE 77) when the box cannot hold the queue.
NEED_KB=$(( 1792 * 1024 ))   # 1 GiB queue + 80 MiB ledger + headroom
AVAIL_KB=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
if [[ "$QUEUE" == "1g" && -z "${SHMCTX_FORCE_BIG:-}" && -n "$AVAIL_KB" && "$AVAIL_KB" -lt "$NEED_KB" ]]; then
  echo "skip: need ${NEED_KB} kB available, have ${AVAIL_KB} kB"; exit 77
fi

"$BIN/shm_create" "$KEY" "$QUEUE" 4 512

"$BIN/shm_consumer" "$KEY" $((PER * PRODUCERS)) 120 > "$LOG" &
CPID=$!
for _ in $(seq 1 100); do grep -q READY "$LOG" && break; sleep 0.05; done
grep -q READY "$LOG" || { echo "consumer never became ready"; exit 1; }

PIDS=()
for p in $(seq 1 $PRODUCERS); do
  "$BIN/shm_producer" "$KEY" "$p" "$PER" 1 & PIDS+=($!)
done
for pid in "${PIDS[@]}"; do wait "$pid"; done

if wait "$CPID"; then CRC=0; else CRC=$?; fi
cat "$LOG"
"$BIN/shm_admin" "$KEY"
[[ $CRC -eq 0 ]] || { echo "consumer failed rc=$CRC"; exit 1; }
echo "cli_pipeline OK"
