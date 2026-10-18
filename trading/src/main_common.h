#pragma once
// Shared main() plumbing for the trading processes:
//   --config <system.ini>      queue keys/sizes + per-component options
//   --create-queues | --destroy-queues
//   --idle-timeout-ms N        exit after N ms without traffic (0 = run until SIGINT/SIGTERM)
//   --verbose                  log every message in/out
// Prints "READY <name>" once subscriptions exist, and the component summary on exit.
#include "shmctx/task.h"   // IniFile, QueueSpec, ApplyQueueKey, EstimateQueueBytes
#include "trading/component.h"

#include <csignal>
#include <cstdio>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <sys/shm.h>

namespace trading {

struct SystemConfig
{
  std::map<std::string, shmctx::QueueSpec> queues;                     // client_bus / order_bus / md_bus
  std::map<std::string, std::map<std::string, std::string>> options;   // [component X] sections

  static SystemConfig Load(const std::string& path)
  {
    SystemConfig c;
    auto ini = shmctx::IniFile::ParseFile(path);
    for (auto* s : ini.SectionsOfKind("queue")) {
      shmctx::QueueSpec q;
      q.name = s->name;
      for (auto& [k, v] : s->values) shmctx::ApplyQueueKey(q, k, v);
      if (q.key.empty()) throw std::runtime_error("[queue " + q.name + "] needs key");
      c.queues[q.name] = q;
    }
    for (auto* s : ini.SectionsOfKind("component")) c.options[s->name] = s->values;
    for (const char* required : { "client_bus", "order_bus", "md_bus" })
      if (!c.queues.count(required)) throw std::runtime_error(std::string("config lacks [queue ") + required + "]");
    return c;
  }

  std::string Opt(const std::string& component, const std::string& key, const std::string& def) const
  {
    auto c = options.find(component);
    if (c == options.end()) return def;
    auto k = c->second.find(key);
    return k == c->second.end() ? def : k->second;
  }
};

struct Args
{
  std::string config;
  bool create = false, destroy = false, verbose = false, help = false;
  int idle_timeout_ms = 0;
  std::map<std::string, std::string> extra;   // component-specific --key value

  static Args Parse(int argc, char** argv)
  {
    Args a;
    for (int i = 1; i < argc; ++i) {
      std::string k = argv[i];
      auto val = [&]() -> std::string { if (i + 1 >= argc) throw std::invalid_argument(k + " needs a value"); return argv[++i]; };
      if (k == "--config") a.config = val();
      else if (k == "--create-queues") a.create = true;
      else if (k == "--destroy-queues") a.destroy = true;
      else if (k == "--verbose") a.verbose = true;
      else if (k == "--idle-timeout-ms") a.idle_timeout_ms = std::stoi(val());
      else if (k == "-h" || k == "--help") a.help = true;
      else if (k.rfind("--", 0) == 0) a.extra[k.substr(2)] = val();
      else throw std::invalid_argument("unexpected argument " + k);
    }
    if (a.config.empty() && !a.help) throw std::invalid_argument("--config <system.ini> is required");
    return a;
  }
};

inline void TouchKeyFile(const std::string& path)
{
  if (FILE* f = std::fopen(path.c_str(), "a")) std::fclose(f);
}

inline int CreateQueues(const SystemConfig& cfg)
{
  for (auto& [name, q] : cfg.queues) {
    TouchKeyFile(q.key);
    mc::ShmContext ctx(q.key, true);
    if (ctx.Create(q.queue_size, q.consumers, q.size_hint) != 0) { std::fprintf(stderr, "Create failed for %s\n", name.c_str()); return 1; }
    std::printf("queue %-10s key=%s storage=%s ledger=%ld consumers=%ld est=%s\n", name.c_str(), q.key.c_str(),
                shmctx::FormatByteSize(ctx.GetStorageSize()).c_str(), ctx.GetLedgerSize(), ctx.GetConsumers(),
                shmctx::FormatByteSize(shmctx::EstimateQueueBytes(q)).c_str());
  }
  return 0;
}

inline int DestroyQueues(const SystemConfig& cfg)
{
  int rc = 0;
  for (auto& [name, q] : cfg.queues) {
    mc::ShmContext ctx(q.key, true);
    int r = ctx.Destroy();
    std::printf("queue %-10s destroyed rc=%d\n", name.c_str(), r);
    if (r != 0) rc = 1;
  }
  return rc;
}

// Attached contexts for one process.
struct AttachedBuses
{
  std::unique_ptr<mc::ShmContext> client, order, md;
  Buses buses;

  explicit AttachedBuses(const SystemConfig& cfg)
  {
    auto attach = [&](const char* name) {
      auto ctx = std::make_unique<mc::ShmContext>(cfg.queues.at(name).key, true);
      if (ctx->Attach() != 0) throw std::runtime_error(std::string("cannot attach ") + name + " (run --create-queues first)");
      return ctx;
    };
    client = attach("client_bus");
    order = attach("order_bus");
    md = attach("md_bus");
    buses.client = client.get();
    buses.order = order.get();
    buses.md = md.get();
  }
};

inline Component* g_running = nullptr;
inline void OnSignal(int) { if (g_running) g_running->RequestStop(); }

// Factory receives the attached buses, the config and parsed args.
using Factory = std::function<std::unique_ptr<Component>(const Buses&, const SystemConfig&, const Args&)>;

inline int RunMain(int argc, char** argv, const char* usage, const Factory& factory)
{
  try {
    Args args = Args::Parse(argc, argv);
    if (args.help) { std::fprintf(stderr, "%s\n%s", argv[0], usage); return 0; }
    SystemConfig cfg = SystemConfig::Load(args.config);
    if (args.create) return CreateQueues(cfg);
    if (args.destroy) return DestroyQueues(cfg);

    AttachedBuses attached(cfg);
    std::unique_ptr<Component> comp = factory(attached.buses, cfg, args);
    comp->SetVerbose(args.verbose);
    if (args.idle_timeout_ms > 0) comp->SetIdleTimeout(std::chrono::milliseconds(args.idle_timeout_ms));

    g_running = comp.get();
    std::signal(SIGINT, OnSignal);
    std::signal(SIGTERM, OnSignal);
    comp->Run([&] { std::printf("READY %s\n", comp->Name().c_str()); std::fflush(stdout); });
    std::printf("%s%s\n", comp->Summary().c_str(), comp->IdleTimedOut() ? " (idle timeout)" : "");
    std::fflush(stdout);
    return 0;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 64;
  }
}

} // namespace trading
