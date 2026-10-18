# shmctx — shared-memory publish/subscribe (`core-libraries/include/shm_context.h` as a standalone project)

A self-contained CMake project around the monorepo's lock-free
multi-producer / multi-consumer shared-memory queue, with examples, a Catch2
unit-test suite and a fork()-based multi-process integration suite.

```
shm_publish_subscribe/
├── CMakeLists.txt                 header-only target shmctx::shmctx (+ install/export)
├── include/shmctx/
│   ├── shmctx.h                   umbrella header (use this)
│   ├── shm_context.h              vendored: ShmContext, ShmProducer, PacketProducer,
│   │                              BatchProducer, ShmConsumer, ShmAdmin, SubscribedTopics
│   ├── shm_queue_types.h          vendored: BufferWrapper, kSkipTopicSentinel
│   ├── spin_lock.h, macros.h      vendored dependencies
│   ├── task.h                     task layer: producer / consumer / relay nodes + pipeline spec
│   ├── topic_mask.h               topic bit-pattern parser (0b.., 0x.., lists, ranges)
│   └── ini.h                      dependency-free INI config reader (${ENV} expansion)
├── examples/                      basic_roundtrip, shm_create, shm_destroy,
│   │                              shm_producer, shm_consumer, shm_admin, shm_task
│   └── configs/                   sample pipelines: fanin_relay, chain, strategy_split
├── tests/
│   ├── support/shm_test_support.h unique SysV key files, Packet payload, helpers
│   ├── unit/                      in-process (private mmap) tests          [unit]
│   ├── integration/               SysV shm + fork()ed processes + CLI     [integration]
│   │                              pipeline_tests drive the shm_task binary  [pipeline]
│   └── large/                     1 GiB queues, serialized via RESOURCE_LOCK [large]
└── scripts/sync_headers.sh        re-vendor headers from the monorepo
```

## Build and test

```bash
cmake -S . -B build            # Catch2 v3 is found via find_package or fetched (v3.7.1)
cmake --build build -j
cd build && ctest --output-on-failure            # 80 tests, ~30 s; big-memory tests run one at a time
ctest -L unit                                   # 59 in-process tests, ~3 s (small queues)
ctest -L pipeline                               # 6 end-to-end shm_task pipelines on 1 GiB queues
ctest -L large                                  # 3 dedicated 1 GiB tests
ctest -L trading                                # algo-trading skeleton: 7 routing tests + multi-process system run
ctest -LE "large|pipeline" -E cli_pipeline      # everything that fits in a few hundred MB
ctest -j8                                       # safe: big-memory tests hold a shared RESOURCE_LOCK
ctest -L integration -E Tier-2                  # skip the slow force-skip test
./tests/unit/shmctx_unit_tests "[topics]"       # Catch2 tag/name filters work directly
cmake -S . -B build-asan -DSHMCTX_SANITIZE=ON   # ASan/UBSan build of examples+tests
```

Requirements: Linux, GCC ≥ 10 or Clang ≥ 12 (C++20), CMake ≥ 3.20, network on first
configure if Catch2 is not installed. The `large`, `pipeline` and `cli_pipeline` tests
create **1 GiB** queues (about 1.1 GiB of RAM each, see *Sizing* below); they share a
ctest `RESOURCE_LOCK` so only one runs at a time, and they `SKIP` (not fail) when
`/proc/meminfo` shows less than about 1.7 GiB available. `SHMCTX_FORCE_BIG=1` runs them
anyway.

## Using the library

```cpp
#include "shmctx/shmctx.h"
using namespace mcp::core;

// Owner process: create the segment. `name` must be an existing file (ftok()).
ShmContext ctx("/var/run/md.key", /*shared_memory=*/true);
ctx.Create(/*queue_size=*/1 << 20, /*num_consumers=*/8, /*size_hint=*/64);

// Any process: attach.
ShmContext ctx2("/var/run/md.key", true);
ctx2.Attach();

// Produce raw bytes / PODs, optionally with topic and strategy id.
ShmProducer p(ctx);               p.ProduceWithTopic(msg, /*topic=*/3);
PacketProducer pp(ctx, seq_off, ts_off);   // stamps seq + ns timestamp into the payload
pp.ProduceWithTopicAndStrategyId(msg, 3, /*strategy_id=*/7);

// Consume: a consumer starts at the message *after* its construction.
ShmConsumer c(ctx2);
BufferWrapper b = c.Consume();    // blocking spin
b = c.TryConsume();               // b.data == nullptr, b.size == -1 when empty
SubscribedTopics subs; subs.set(3); subs.set_strategy_ids(uint16_t{7});
b = c.TryConsume(subs);           // skips non-matching slots, advances cursor to head+1

ShmAdmin a(ctx);                  // GetCurrentSequenceNumber / GetClaimedSequenceNumber / stats
ctx.Destroy();                    // IPC_RMID (owner)
```

Pass `shared_memory=false` to get a private anonymous mmap: same API, single process,
ideal for unit tests and simulators.

### Geometry and sizing

`Create(queue_size, num_consumers, size_hint)` rounds `queue_size` up to a power of two
(the storage ring) and sets `ledger = storage / pow2(size_hint)` slots. There is **no
back-pressure**: a producer never waits for consumers, so a consumer that falls more
than `ledger` messages (or `storage` bytes) behind reads overwritten data. Size the ring
for the worst-case lag.

Memory is committed up front (`Create` zero-fills the whole segment):

```
segment = storage + ledger_slots * 40 B + consumers * 64 B + header
```

| queue_size | size_hint | ledger slots | segment | Create time here |
|---|---|---|---|---|
| 1 MiB | 64 | 16384 | 1.6 MiB | < 1 ms |
| 1 GiB | 64 | 16.8 M | **1.63 GiB** | ~2.5 s |
| 1 GiB | 512 | 2.1 M | 1.08 GiB | ~1.5 s |
| 2 GiB | 64 | 33.5 M | **3.5 GiB** | 5.2 s |
| 2 GiB | 512 | 4.2 M | 2.16 GiB | ~3 s |

For GiB-scale queues raise `size_hint` to keep the ledger reasonable; 2M slots is still
2M messages of consumer lag. `shm_task --list` / `--dry-run` print `est_segment` per
queue, and the runner prints the kernel's real segment size after `Create`. Sizes in
configs and on the CLI accept `k`/`m`/`g` suffixes (`1g`, `256m`); the sample configs use
`${SHMCTX_QUEUE_SIZE:-1g}` so `SHMCTX_QUEUE_SIZE=64m shm_task --config ...` runs them on
a small machine.

## Task layer: producer / consumer / relay nodes (`shmctx/task.h`, `shm_task`)

A **task** is a configurable node over one or two queues. The `relay` type is both
a consumer and a producer: it reads a topic bit-pattern from one queue, transforms the
message (hop count, relay id, re-sealed checksum) and republishes it on another topic,
possibly on the same queue.

| type | reads | writes | stops when |
|---|---|---|---|
| `producer` | – | `count` messages on `topic` / `strategy_id`, then one End marker | done |
| `consumer` | `subscribe` mask (+ `strategy_ids`) | – | `ends` End markers seen, or `expect` messages, or `timeout_s` |
| `relay` | `subscribe` mask on `in` | every match on `out` / `out_topic`, End markers forwarded | `ends` End markers forwarded, or `timeout_s` |

Every consumer-side task verifies checksums, per-origin ordering (`check_order`), and
reports lag: `max_lag` is the largest distance behind the queue head it observed, and
`overruns` counts reads made while that distance was ≥ the ledger size (the slot may
already have been overwritten, since the queue has no back-pressure). Size
`queue_size / size_hint` above the `max_lag` you see.

### Topic bit patterns

`--subscribe` / `subscribe =` accept, case-insensitively:

| spelling | meaning |
|---|---|
| `0b0110` | binary mask, bit *i* = topic *i* (LSB = topic 0) |
| `0x6` | hex mask |
| `1,2,8-11` | list of topics and inclusive ranges |
| `5` | the single topic 5 (a bare number is a topic, not a mask) |
| `all`, `*`, `none` | every topic / empty |
| `0x3\|8-9\|all` | groups joined with `\|` or `+` are OR-ed |

Masks print back as `0x6 {1-2}`, and that form is accepted as input again.
`strategy_ids` takes `7,9`, `all` (= strategy 0, every strategy) or `none` (broadcast only).

### Command line

Flags share the INI key names (`-` and `_` interchangeable). Without a config the queue
references are key-file paths:

```bash
B=build/examples
$B/shm_create /tmp/md.key 8388608 8 64 ; $B/shm_create /tmp/strat.key
$B/shm_task --type consumer --queue /tmp/strat.key --subscribe 5 --ends 2 &
$B/shm_task --type relay --in /tmp/md.key --subscribe 0b0110 --out /tmp/strat.key --out-topic 5 --id 10 --ends 2 &
$B/shm_task --type producer --queue /tmp/md.key --id 1 --topic 1 --count 20000 --size 96
$B/shm_task --type producer --queue /tmp/md.key --id 2 --topic 2 --count 20000
```

Each task prints `READY <name>` once its cursor is registered and one summary line at
the end, e.g.

```
relay[relay] ok=1 produced=40000 received=40000 bad=0 misordered=0 ends=2 origins=2 max_hops=0 max_lag=1619 overruns=0 max_latency_us=804.0 elapsed_ms=25.91
```

Exit codes: 0 ok, 2 task failed its checks, 64 bad arguments/config.

### Config file

```ini
[pipeline]
ready_timeout_s = 10

[queue md]                       ; referenced by name from tasks
key        = ${TMPDIR:-/tmp}/shmctx_md.key   ; ${VAR} / ${VAR:-default} expand
queue_size = 8388608
consumers  = 8
size_hint  = 64
create     = true                ; the pipeline runner creates and destroys it

[task feed_btc]
type = producer
queue = md
id = 1
topic = 1
count = 20000
size = 96                        ; bytes per message (>= 56 header)

[task relay]
type = relay
in = md
subscribe = 0b0110               ; topics 1 and 2
out = strat
out_topic = 5
id = 10
ends = 2                         ; two upstream producers

[task sink]
type = consumer
queue = strat
subscribe = 5
ends = 2
timeout_s = 30
```

Task keys: `type queue in out id topic out_topic strategy_id subscribe strategy_ids
count size pause_us expect ends check_order timeout_s verbose`.
Queue keys: `key queue_size consumers size_hint create` (sizes accept `k`/`m`/`g`).

```bash
$B/shm_task --config examples/configs/fanin_relay.ini      # create queues, fork all tasks
                                                           # (sinks/relays first, wait READY,
                                                           # then producers), wait, destroy
$B/shm_task --config f.ini --list                          # resolved queues + tasks
$B/shm_task --config f.ini --dry-run
$B/shm_task --config f.ini --create-queues                 # then run tasks by hand:
$B/shm_task --config f.ini --task relay --subscribe 0x2    #   one task, flags override the file
$B/shm_task --config f.ini --destroy-queues
```

Validation rejects a relay that reads and writes the same queue while its mask includes
its own `out_topic` (it would loop forever), empty masks, topics ≥ 64, undersized
messages, unknown keys and duplicate names.

Sample pipelines in `examples/configs/`: `fanin_relay.ini` (2 feeds + noise → relay →
sink), `chain.ini` (3 hops on one queue, `max_hops=2` at the sink), `strategy_split.ini`
(strategy-id routing; broadcast End markers reach filtered consumers).

Programmatic use (what the unit tests do, in-process with threads):

```cpp
#include "shmctx/task.h"
shmctx::TaskSpec relay; relay.type = shmctx::TaskType::Relay; relay.in = "a"; relay.out = "b";
relay.subscribe = shmctx::ParseTopicMask("0b0110"); relay.out_topic = 5; relay.ends = 2;
shmctx::TaskStats st = shmctx::RunRelay(ctx_a, ctx_b, relay, /*on_ready=*/[&] { latch.count_down(); });
```

## Semantics covered by the tests

| Area | Tests |
|---|---|
| Lifecycle / geometry | power-of-two rounding, private vs SysV mode, Attach/Destroy, Attach on missing segment |
| Produce / consume | FIFO, variable sizes, late-consumer cursor, TryConsume miss, storage wrap-around (payload never straddles the end), batch concatenation |
| Topics / strategy ids | `SubscribedTopics` bit ops, `IsIncluded` truth table, filtered `TryConsume` cursor advance, strategy filtering, topic 63 vs 64 limit |
| Wrappers | `PacketProducer` seq + timestamp stamping (scalar, raw, batch), `BatchProducer` |
| Concurrency (threads) | 4 producers × 5000 msgs (per-producer order, no loss/dup), gap-free sequence across threads, 3 broadcast consumers, lagging consumer |
| Multi-process (SysV) | child producer → parent consumer, 3 producer processes, producer + consumer children, blocking `Consume()` wake-up, topic-filtered child, `ShmAdmin` from another process |
| Crash recovery | Tier-1 (stamped pid dead → skipped in ~100 ms), topic-filtered skip, blocked remote consumer released, 3 dead producers in a row, Tier-2 (unstamped → force-skip after 10 s) |
| End to end | `cli_pipeline.sh`: shm_create → shm_consumer → 3 × shm_producer → shm_admin → shm_destroy |
| Task layer (unit) | mask parsing/formatting round-trip and rejects, INI parsing (comments, quotes, `${ENV}`, line-numbered errors), `LoadPipeline` defaults + validation, CLI-style overrides, producer→consumer, mask + strategy filtering, `expect`, relay A→B with hops/relay id, 3-hop chain on one queue, relay timeout, `RunTask` dispatch, lag/overrun detection |
| Task layer (pipeline) | `shm_task --config` on 1 GiB queues: fan-in relay, 3-hop chain (300k msgs × 3 streams), strategy routing, failing sink → `pipeline FAILED` + cleanup, flag-only consumer/relay/producer processes, `--list` / `--dry-run` / overrides / bad input exit codes |
| Trading skeleton | `trading/`: hop-by-hop routing counters for simple and algo orders, owner-id filtering of exec reports, cancels, partial fills, market-data fan-out, persister journal completeness; `run_system.sh` runs the 7 processes over SysV and checks 12 journal lines per order |
| Large (1 GiB) | private 1 GiB ring: geometry, 1025 × 1 MiB messages crossing the 1 GiB boundary intact; 1M-message burst fully buffered before the consumer starts (`max_lag ≥ 1M`, `overruns = 0`); 1 GiB SysV segment with 4 producer processes × 250k × 256 B drained and verified by the parent |

The crash-recovery tests need to stop a producer half-way through `Produce()`. The
vendored header therefore carries one guarded addition:

```cpp
#ifdef SHMCTX_ENABLE_TEST_HOOKS
  friend struct ShmContextTestHooks;
#endif
```

It is only compiled into `shmctx_integration_tests`; production builds are unchanged.
`scripts/sync_headers.sh` re-copies the four headers from the monorepo and re-applies
this hook plus the `shmctx/`-rooted include paths.

## Findings about the library (pinned by tests or documented)

1. **`PacketProducer::Produce(const T&)` does not compile when used.** It forwards to
   `ShmProducer::ProduceWithSeqAndTimestamp(T&)`, which deduces `T = const X` and then
   `reinterpret_cast<char*>` casts away const. Production only uses the raw-pointer and
   `ProduceWithTopic*` overloads, so this has gone unnoticed. The tests use the raw
   overload. Fix: take `T&` in `PacketProducer::Produce`.
2. **`BatchProducer::SendBatch()` never clears on success.** It checks `if (!out) return out;`
   where `out == 0` means success, so a successful send returns before `offset_ = 0`.
   Callers must call `Clear()` themselves. Pinned by a unit test so a fix is noticed.
3. **Topics must be `< 64`.** The ledger stores a `uint16_t` topic but the subscription is
   a `std::bitset<64>`; publishing topic ≥ 64 makes `TryConsume(topics)` throw
   `std::out_of_range`. Pinned by a unit test.
4. **Recovery relies on `kill(pid, 0) == ESRCH`.** A zombie still answers `kill(0)`
   successfully, so a dead producer must be reaped by its parent before Tier-1
   recovery can act. The integration tests `waitpid()` the rogue child first.
5. **`ShmContext::GetCurrentSequenceNumber()` / `GetClaimedSequenceNumber()` /
   `GetConsumerStats()` are private**; reach them through `ShmAdmin` (or
   `ShmContext::GetCursor()` for the committed head).
6. Consumer slots (`ConsumerSequence`, `occupied`, `pid`) are allocated but never
   written by `ShmConsumer`, so `ShmAdmin::GetConsumerStats` always reports zero
   occupancy. `GetMinConsumerSequence()` consequently returns `INT64_MAX`.
7. **No back-pressure, silently.** With a 1 MiB / 64 B queue (16384 ledger slots), three
   producers bursting 45k messages lapped a relay before it could forward them; the relay
   then read overwritten slots (637 bad checksums, nonsense hop counts). Nothing in the
   library signals this. The task layer now reports `max_lag` / `overruns` and the sample
   configs use 1 GiB queues; see *Geometry and sizing*.
8. **Ledger memory is easy to underestimate.** `size_hint` is not a hint: it fixes the
   slot count, and each slot is 40 bytes that `Create` zero-fills. A 2 GiB queue with the
   usual `size_hint = 64` costs 3.5 GiB and 5 s to create.

## Example binaries

```bash
B=build/examples
$B/shm_create  /tmp/md.key 1048576 4 64         # creates key file + segment
$B/shm_consumer /tmp/md.key 12000 30 1 &        # prints READY, exits 0 iff all 12000 verified
$B/shm_producer /tmp/md.key 1 4000 1            # producer-id 1, 4000 msgs, topic 1
$B/shm_producer /tmp/md.key 2 4000 1
$B/shm_producer /tmp/md.key 3 4000 1
$B/shm_admin    /tmp/md.key                     # committed/claimed seq, in-flight
$B/shm_destroy  /tmp/md.key
$B/basic_roundtrip                              # in-process: 1 producer, 2 consumer threads
```

`ipcs -m` lists segments; the test fixture removes its own segments and key files
(`SHMCTX_TEST_TMPDIR` overrides the directory used for key files).
