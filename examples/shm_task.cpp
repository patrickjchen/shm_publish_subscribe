// shm_task: run one producer / consumer / relay node, or a whole pipeline.
//
//   # one node, everything from flags (queues are key-file paths)
//   shm_task --type producer --queue /tmp/md.key --id 1 --topic 1 --count 10000
//   shm_task --type relay --in /tmp/md.key --subscribe 0b0110 --out /tmp/strat.key --out-topic 5
//   shm_task --type consumer --queue /tmp/strat.key --subscribe 5 --ends 2
//
//   # pipeline from a config file: creates queues, forks every task, waits, destroys
//   shm_task --config examples/configs/fanin_relay.ini
//   shm_task --config f.ini --task relay --subscribe 0x2      # one task from the file, flag overrides
//   shm_task --config f.ini --create-queues | --destroy-queues | --list | --dry-run
//
// Flags use the same names as INI keys; '-' and '_' are interchangeable.
#include "shmctx/task.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <sys/select.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace shmctx;
namespace mc = mcp::core;

namespace {

struct Cli
{
  std::string config;
  std::string task;                           // --task <name>
  bool list = false, dry_run = false, create_queues = false, destroy_queues = false, help = false;
  std::vector<std::pair<std::string, std::string>> task_kv;   // ordered overrides / direct spec
};

void Usage(const char* argv0)
{
  std::fprintf(stderr,
    "usage: %s [--config <file.ini>] [--task <name>] [--list|--dry-run|--create-queues|--destroy-queues]\n"
    "          [--type producer|consumer|relay] [--queue|--in|--out <name-or-keyfile>]\n"
    "          [--subscribe <mask>] [--topic N] [--out-topic N] [--strategy-id N] [--strategy-ids list|all]\n"
    "          [--id N] [--count N] [--size BYTES] [--pause-us N] [--expect N] [--ends N]\n"
    "          [--check-order bool] [--timeout-s N] [--verbose]\n"
    "mask: all | none | 0b0110 | 0x6 | 1,2,8-11 | 5   (groups joinable with '|')\n", argv0);
}

Cli ParseCli(int argc, char** argv)
{
  Cli c;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "-h" || a == "--help") { c.help = true; continue; }
    if (a.rfind("--", 0) != 0) throw std::invalid_argument("unexpected argument '" + a + "'");
    std::string key = a.substr(2), val;
    bool has_val = false;
    if (auto eq = key.find('='); eq != std::string::npos) { val = key.substr(eq + 1); key = key.substr(0, eq); has_val = true; }
    for (auto& ch : key) if (ch == '-') ch = '_';
    auto need = [&]() -> std::string {
      if (has_val) return val;
      if (i + 1 >= argc) throw std::invalid_argument("--" + key + " needs a value");
      return argv[++i];
    };
    if (key == "config") c.config = need();
    else if (key == "task") c.task = need();
    else if (key == "list") c.list = true;
    else if (key == "dry_run") c.dry_run = true;
    else if (key == "create_queues") c.create_queues = true;
    else if (key == "destroy_queues") c.destroy_queues = true;
    else if (key == "verbose" && !has_val && (i + 1 >= argc || std::string(argv[i + 1]).rfind("--", 0) == 0))
      c.task_kv.emplace_back("verbose", "true");
    else c.task_kv.emplace_back(key, need());
  }
  return c;
}

void TouchKeyFile(const std::string& path)
{
  if (FILE* f = std::fopen(path.c_str(), "a")) std::fclose(f);
}

// Attached/created contexts, keyed by key-file path, for one process.
class QueueRegistry
{
public:
  explicit QueueRegistry(const PipelineSpec& p) : pipeline_(p) {}

  mc::ShmContext& Attach(const std::string& ref)
  {
    std::string key = pipeline_.ResolveKey(ref);
    auto it = ctx_.find(key);
    if (it != ctx_.end()) return *it->second;
    auto ctx = std::make_unique<mc::ShmContext>(key, true);
    if (ctx->Attach() != 0) throw std::runtime_error("cannot attach to queue '" + ref + "' (key file " + key + "); create it first");
    return *ctx_.emplace(key, std::move(ctx)).first->second;
  }

  void CreateAll()
  {
    for (auto& q : pipeline_.queues) {
      if (!q.create) continue;
      TouchKeyFile(q.key);
      auto ctx = std::make_unique<mc::ShmContext>(q.key, true);
      if (ctx->Create(q.queue_size, q.consumers, q.size_hint) != 0) throw std::runtime_error("Create failed for queue '" + q.name + "'");
      // Report the real segment size from the kernel.
      struct shmid_ds ds{};
      int id = ::shmget(::ftok(q.key.c_str(), 8), 0, 0);
      std::string seg = (id >= 0 && ::shmctl(id, IPC_STAT, &ds) == 0) ? FormatByteSize(int64_t(ds.shm_segsz)) : "?";
      std::printf("queue %-12s created key=%s storage=%s ledger=%ld consumers=%ld segment=%s\n", q.name.c_str(), q.key.c_str(),
                  FormatByteSize(ctx->GetStorageSize()).c_str(), ctx->GetLedgerSize(), ctx->GetConsumers(), seg.c_str());
      created_.emplace(q.key, std::move(ctx));
    }
  }
  void DestroyAll()
  {
    for (auto& q : pipeline_.queues) {
      if (!q.create) continue;
      mc::ShmContext ctx(q.key, true);
      int rc = ctx.Destroy();
      std::printf("queue %-12s destroyed rc=%d\n", q.name.c_str(), rc);
    }
    created_.clear();
  }

private:
  const PipelineSpec& pipeline_;
  std::map<std::string, std::unique_ptr<mc::ShmContext>> ctx_;
  std::map<std::string, std::unique_ptr<mc::ShmContext>> created_;
};

int RunOne(const PipelineSpec& p, const TaskSpec& t, bool announce_ready)
{
  QueueRegistry reg(p);
  std::fprintf(stderr, "task %s\n", DescribeTask(t).c_str());
  auto stats = RunTask(t, [&](const std::string& ref) -> mc::ShmContext& { return reg.Attach(ref); },
                       [&] { if (announce_ready) { std::printf("READY %s\n", t.name.c_str()); std::fflush(stdout); } });
  std::printf("%s\n", stats.ToString().c_str());
  std::fflush(stdout);
  return stats.ok ? 0 : 2;
}

// Fork every task: sinks/relays first (wait for READY on a pipe), then sources.
int RunPipeline(const PipelineSpec& p)
{
  QueueRegistry reg(p);
  reg.CreateAll();

  struct Child { pid_t pid; std::string name; int rc = -1; };
  std::vector<Child> kids;
  bool ok = true;

  auto launch = [&](const TaskSpec& t) {
    int fds[2];
    if (::pipe(fds) != 0) { std::perror("pipe"); std::exit(1); }
    std::fflush(nullptr);   // children must not inherit (and re-flush) our buffered output
    pid_t pid = ::fork();
    if (pid < 0) { std::perror("fork"); std::exit(1); }
    if (pid == 0) {
      ::close(fds[0]);
      int rc = 1;
      try {
        QueueRegistry creg(p);
        auto stats = RunTask(t, [&](const std::string& ref) -> mc::ShmContext& { return creg.Attach(ref); },
                             [&] { char c = 'R'; (void)!::write(fds[1], &c, 1); ::close(fds[1]); });
        std::printf("%s\n", stats.ToString().c_str());
        rc = stats.ok ? 0 : 2;
      } catch (const std::exception& e) {
        std::fprintf(stderr, "task %s failed: %s\n", t.name.c_str(), e.what());
        rc = 3;
      }
      std::fflush(nullptr);
      ::_exit(rc);
    }
    ::close(fds[1]);
    // Wait for READY (sources signal immediately too, keeps the protocol uniform).
    fd_set set; FD_ZERO(&set); FD_SET(fds[0], &set);
    timeval tv{ p.ready_timeout_s, 0 };
    char c = 0;
    bool ready = ::select(fds[0] + 1, &set, nullptr, nullptr, &tv) > 0 && ::read(fds[0], &c, 1) == 1;
    ::close(fds[0]);
    std::printf("start %-12s pid=%d %s%s\n", t.name.c_str(), pid, DescribeTask(t).c_str(), ready ? "" : "  (NOT READY)");
    if (!ready) ok = false;
    kids.push_back({ pid, t.name });
  };

  for (auto& t : p.tasks) if (!t.IsSource()) launch(t);
  for (auto& t : p.tasks) if (t.IsSource()) launch(t);

  for (auto& k : kids) {
    int status = 0;
    ::waitpid(k.pid, &status, 0);
    k.rc = WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
    if (k.rc != 0) ok = false;
    std::printf("exit  %-12s rc=%d\n", k.name.c_str(), k.rc);
  }
  reg.DestroyAll();
  std::printf("pipeline %s\n", ok ? "OK" : "FAILED");
  return ok ? 0 : 1;
}

} // namespace

int main(int argc, char** argv)
{
  try {
    Cli cli = ParseCli(argc, argv);
    if (cli.help) { Usage(argv[0]); return 0; }

    PipelineSpec pipeline;
    if (!cli.config.empty()) pipeline = LoadPipeline(IniFile::ParseFile(cli.config));

    if (cli.list) {
      for (auto& q : pipeline.queues)
        std::printf("queue %s key=%s storage=%s create=%d est_segment=%s\n", q.name.c_str(), q.key.c_str(),
                    FormatByteSize(q.queue_size).c_str(), q.create, FormatByteSize(EstimateQueueBytes(q)).c_str());
      for (auto& t : pipeline.tasks) std::printf("task  %s\n", DescribeTask(t).c_str());
      return 0;
    }
    if (cli.create_queues) { QueueRegistry(pipeline).CreateAll(); return 0; }
    if (cli.destroy_queues) { QueueRegistry(pipeline).DestroyAll(); return 0; }

    // Single task: from the file (with overrides) or purely from flags.
    if (!cli.task.empty() || !cli.task_kv.empty()) {
      TaskSpec t;
      if (!cli.task.empty()) {
        auto* found = pipeline.FindTask(cli.task);
        if (!found) throw std::invalid_argument("no [task " + cli.task + "] in " + cli.config);
        t = *found;
      } else {
        t.name = "cli";
      }
      for (auto& [k, v] : cli.task_kv) ApplyTaskKey(t, k, v);
      ValidateTask(t);
      if (cli.dry_run) { std::printf("%s\n", DescribeTask(t).c_str()); return 0; }
      return RunOne(pipeline, t, /*announce_ready=*/true);
    }

    if (cli.config.empty()) { Usage(argv[0]); return 1; }
    if (cli.dry_run) {
      for (auto& q : pipeline.queues)
        std::printf("queue %s key=%s size=%s consumers=%ld hint=%ld create=%d est_segment=%s\n", q.name.c_str(), q.key.c_str(),
                    FormatByteSize(q.queue_size).c_str(), q.consumers, q.size_hint, q.create, FormatByteSize(EstimateQueueBytes(q)).c_str());
      for (auto& t : pipeline.tasks) std::printf("task  %s\n", DescribeTask(t).c_str());
      return 0;
    }
    if (pipeline.tasks.empty()) throw std::invalid_argument("config has no [task ...] sections");
    return RunPipeline(pipeline);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 64;
  }
}
