# trading/ — algo trading system skeleton (message routing only)

Seven processes connected by three `shmctx` shared-memory buses. Every component is a
`trading::Component` subclass: it subscribes to topics on the buses it cares about,
handles messages in `OnMessage()`, and publishes with `Publish()`. **No business
logic**: order validation, slicing, venue selection, position keeping and persistence
to a database are virtual hooks or stubs, clearly marked in each header.

```
                     client_bus                              order_bus                               md_bus
 ┌────────┐ ClientOrder ┌─────────┐ SimpleOrder ┌─────┐ EmsRequest ┌─────┐ ExchangeOut ┌──────────┐
 │ client │────────────▶│ order   │────────────▶│ OMS │───────────▶│     │────────────▶│ exchange │
 │  (N)   │◀────────────│ entry   │             └─────┘            │ EMS │◀────────────│   sim    │
 └────────┘ ClientReport│ gateway │ AlgoOrder   ┌──────┐           │     │ ExchangeIn  └──────────┘
   dest=client_id       │         │────────────▶│ algo │──────────▶│     │
                        └─────────┘             │engine│           └─────┘
                             ▲ OwnerReport      └──────┘              │ ExecReport, dest = owner (OMS=1 / algo=2)
                             └──────────────────────┴─────────────────┘
                                                    ▲ BookTicker / Trade (md_bus)
                                              ┌─────┴──────┐
                                              │ market data│   ┌───────────┐  subscribes *all* topics on
                                              │  handler   │   │ persister │  client_bus + order_bus,
                                              └────────────┘   └───────────┘  journals one line per message
```

| component | binary | consumes | publishes |
|---|---|---|---|
| order entry gateway | `trading_gateway` | client_bus `ClientOrder`; order_bus `OwnerReport` | order_bus `SimpleOrder` (algo = None) or `AlgoOrder`; client_bus `ClientReport` **dest = client_id** |
| OMS | `trading_oms` | order_bus `SimpleOrder`, `ExecReport` **dest = 1**; md_bus | order_bus `EmsRequest` (ChildOrder / ChildCancel, owner = 1), `OwnerReport` |
| algo engine | `trading_algo_engine` | order_bus `AlgoOrder`, `ExecReport` **dest = 2**; md_bus | order_bus `EmsRequest` (N children, owner = 2), `OwnerReport` |
| EMS | `trading_ems` | order_bus `EmsRequest`, `ExchangeIn` | order_bus `ExchangeOut`; `ExecReport` **dest = child.owner** |
| exchange simulator | `trading_exchange_sim` | order_bus `ExchangeOut` | order_bus `ExchangeIn` (ack, fill / partial / cancel per `fill_mode`) |
| market data handler | `trading_md_handler` | (venue feed: `OnRawFeed()` hook; `sim` mode synthesizes) | md_bus `BookTicker`, `Trade` |
| persister | `trading_persister` | all topics on client_bus + order_bus (+ md_bus with `log_md`) | append-only journal file |
| client simulator | `trading_client_sim` | client_bus `ClientReport` dest = its id | client_bus `ClientOrder` |

**Routing rules.** Topics say *what* a message is; the SHM `strategy_id` field is used as
a *destination id* (`dest` in the journal). The EMS publishes every `ExecReport` with
`dest = owner`, so the OMS subscribes `{SimpleOrder, ExecReport}` with owner filter `{1}`
and the algo engine with `{2}`: each sees its own reports only, while broadcast traffic
(dest 0) passes both filters. The gateway publishes `ClientReport` with
`dest = client_id`, so any number of clients can share client_bus. Cancels follow the
same route as the order they refer to (gateway → owner → EMS → exchange).

## Files

```
trading/
├── include/trading/
│   ├── messages.h                 wire structs (POD, Header first: seq@0, ts@8), Topic/Owner/MsgType, Describe()
│   ├── component.h                Component base: Buses, Subscribe(), Publish(), PollOnce()/Run(), counters
│   └── components/
│       ├── order_entry_gateway.h  ValidateOrder() hook
│       ├── oms.h                  OnMarketData() hook
│       ├── algo_engine.h          Slice() + OnMarketData() hooks (skeleton: one child per parent)
│       ├── ems.h                  (venue selection hook would go here)
│       ├── exchange_sim.h         fill_mode = full | ack | partial
│       ├── market_data_handler.h  OnRawFeed() hook, sim mode
│       ├── persister.h            journal writer (database hook)
│       └── client.h               SendOrder() / Cancel() / Poll(), per-order state
├── src/*_main.cpp + main_common.h one process per component; --config, --create-queues, --idle-timeout-ms, --verbose
├── config/system.ini              three [queue ...] sections + [component ...] options
├── scripts/run_system.sh          create queues -> start all -> client_sim -> stop -> journal summary
└── tests/routing_tests.cpp        in-process end-to-end routing (threads on private buses)
```

## Run it

```bash
cmake --build build -j
build/trading/../trading/scripts/run_system.sh build/trading trading/config/system.ini 1000
# or by hand:
B=build/trading; C=trading/config/system.ini
$B/trading_gateway --config $C --create-queues
$B/trading_persister --config $C --out /tmp/journal.log &
$B/trading_exchange_sim --config $C &  $B/trading_ems --config $C &
$B/trading_oms --config $C &           $B/trading_algo_engine --config $C &
$B/trading_gateway --config $C &       $B/trading_md_handler --config $C &
$B/trading_client_sim --config $C --orders 1000 --algo-ratio 0.5
kill %1 %2 %3 %4 %5 %6 %7; $B/trading_gateway --config $C --destroy-queues
```

Each process prints `READY <name>` once subscribed and a per-topic summary on exit, e.g.
`ems: in=3000 out=3000 in.EmsRequest=1000 in.ExchangeIn=2000 out.ExchangeOut=1000 out.ExecReport=2000`.
The journal has one self-describing line per message:

```
order_bus seq=812 ts=1760800000123456789 topic=ExecReport dest=2 src=40 ExecReport order=2814749767106561 child=8444249301319681 exch_ord=11258999068426241 owner=2 client=7 Filled fill=3.0000@104.0000 cum=3.0000 leaves=0.0000
```

Start consumers before producers (the script does): a `ShmConsumer` begins at the
message after its creation. Bus sizing follows the shmctx rules (no back-pressure, ledger
= `queue_size / size_hint` slots); the persister is the slowest reader, so size order_bus
for its worst-case lag.

## Tests

```bash
cd build && ctest -L trading          # 7 in-process routing tests + trading.system (multi-process via run_system.sh)
```

The in-process suite runs every component in its own thread on private buses and asserts
the hop-by-hop counters (e.g. an algo order never touches the OMS, OMS reports never
reach the algo engine), the client-side state machine (New → Filled, partial fills
aggregated per parent, cancels), market-data fan-out (OMS + algo only), and that the
persister journaled exactly one line per message with the expected `dest` ids.

## Where business logic goes

- `OrderEntryGateway::ValidateOrder()` — pre-trade risk, throttles, symbol checks.
- `AlgoEngine::Slice()` / `OnMarketData()` — TWAP/VWAP/POV scheduling (skeleton emits one child).
- `Oms::OnMarketData()` — marking, positions, limits.
- `Ems` — venue selection / smart routing before `ExchangeOut`; real venue adapters replace `ExchangeSim`.
- `MarketDataHandler::OnRawFeed()` — feed decoding; the sim generator shows the publish side.
- `Persister::OnMessage()` — swap the line writer for batched database inserts.
