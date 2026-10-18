#!/usr/bin/env bash
# Bring up the whole skeleton as separate processes, push orders through it,
# then tear it down.   usage: run_system.sh <bin-dir> [system.ini] [orders]
set -euo pipefail
BIN="${1:?bin dir}"
CFG="${2:-$(dirname "$0")/../config/system.ini}"
ORDERS="${3:-1000}"
LOGDIR="$(mktemp -d "${TMPDIR:-/tmp}/trading_run_XXXXXX")"
export TMPDIR="${TMPDIR:-/tmp}"
PIDS=()

cleanup() {
  for p in "${PIDS[@]:-}"; do [[ -n "$p" ]] && kill -TERM "$p" 2>/dev/null || true; done
  for p in "${PIDS[@]:-}"; do [[ -n "$p" ]] && wait "$p" 2>/dev/null || true; done
  "$BIN/trading_gateway" --config "$CFG" --destroy-queues >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "== create queues"
"$BIN/trading_gateway" --config "$CFG" --create-queues

start() {   # start <binary> [args...]; waits for its READY line
  local name="$1"; shift
  "$BIN/$name" --config "$CFG" "$@" > "$LOGDIR/$name.log" 2>&1 &
  PIDS+=($!)
  for _ in $(seq 1 200); do grep -q '^READY' "$LOGDIR/$name.log" 2>/dev/null && break; sleep 0.02; done
  grep -q '^READY' "$LOGDIR/$name.log" || { echo "$name did not become ready"; cat "$LOGDIR/$name.log"; exit 1; }
  echo "   up: $name (pid ${PIDS[-1]})"
}

echo "== start components (consumers first)"
JOURNAL="$LOGDIR/journal.log"
start trading_persister --out "$JOURNAL"
start trading_exchange_sim
start trading_ems
start trading_oms
start trading_algo_engine
start trading_gateway
start trading_md_handler

echo "== client: $ORDERS orders"
"$BIN/trading_client_sim" --config "$CFG" --orders "$ORDERS" | tee "$LOGDIR/client.log"
CRC=${PIPESTATUS[0]}

echo "== stop components (upstream first, persister last; each drains its backlog on SIGTERM)"
for (( i=${#PIDS[@]}-1; i>=0; i-- )); do kill -TERM "${PIDS[$i]}"; wait "${PIDS[$i]}" || true; done
PIDS=()
for f in md_handler gateway algo_engine oms ems exchange_sim persister; do grep -v '^READY' "$LOGDIR/trading_$f.log" | tail -1; done

echo "== journal: $JOURNAL ($(wc -l < "$JOURNAL") lines)"
awk '{for(i=1;i<=NF;i++) if($i ~ /^topic=/) {t[$i]++}} END {for (k in t) printf "   %-22s %d\n", k, t[k]}' "$JOURNAL" | sort
echo "   first 3 lines:"; head -3 "$JOURNAL" | sed 's/^/   /'
echo "   client rc=$CRC"
# Full-fill mode, no cancels: 12 journal lines per order
# (ClientOrder, Simple|AlgoOrder, EmsRequest, ExchangeOut, 2 ExchangeIn, 2 ExecReport, 2 OwnerReport, 2 ClientReport).
LINES=$(wc -l < "$JOURNAL"); EXPECT=$(( ORDERS * 12 ))
if [[ "$LINES" -ne "$EXPECT" ]]; then echo "   journal incomplete: $LINES lines, expected $EXPECT"; CRC=1; else echo "   journal complete: $LINES = 12 x $ORDERS"; fi
[[ $CRC -eq 0 ]] && echo "== SYSTEM OK" || { echo "== SYSTEM FAILED"; exit 1; }
