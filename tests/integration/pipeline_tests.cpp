// End-to-end: drive the shm_task binary with generated INI configs and flags.
// Each test writes its config to a temp dir, runs `shm_task --config ...`
// (which forks one process per task over real SysV segments) and checks the
// exit code plus the per-task summary lines.
#include "process_utils.h"
#include "shm_test_support.h"
#include "shmctx/task.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <fcntl.h>
#include <filesystem>
#include <fstream>

#ifndef SHM_TASK_BIN
#error "SHM_TASK_BIN must point at the shm_task executable"
#endif

using namespace shmctx;
using namespace shmctx_test;
namespace fs = std::filesystem;

namespace {

struct TempDir
{
  fs::path path;
  TempDir()
  {
    const char* base = std::getenv("SHMCTX_TEST_TMPDIR");
    std::string tmpl = ((base ? fs::path(base) : fs::temp_directory_path()) / "shmctx_pipe_XXXXXX").string();
    REQUIRE(::mkdtemp(tmpl.data()) != nullptr);
    path = tmpl;
  }
  ~TempDir()
  {
    // Remove any segment the pipeline may have left behind, then the dir.
    for (auto& e : fs::directory_iterator(path)) {
      key_t k = ::ftok(e.path().c_str(), 8);
      if (k != -1) { int id = ::shmget(k, 0, 0); if (id >= 0) ::shmctl(id, IPC_RMID, nullptr); }
    }
    fs::remove_all(path);
  }
  std::string Write(const std::string& name, const std::string& text) const
  {
    auto p = path / name;
    std::ofstream(p) << text;
    return p.string();
  }
};

struct RunResult { int rc; std::string out; };

// Run shm_task with args, capture stdout+stderr.
RunResult RunShmTask(const std::vector<std::string>& args, std::chrono::seconds timeout = std::chrono::seconds(60))
{
  int fds[2];
  REQUIRE(::pipe(fds) == 0);
  pid_t pid = ::fork();
  REQUIRE(pid >= 0);
  if (pid == 0) {
    ::dup2(fds[1], STDOUT_FILENO);
    ::dup2(fds[1], STDERR_FILENO);
    ::close(fds[0]); ::close(fds[1]);
    std::vector<char*> argv;
    std::string bin = SHM_TASK_BIN;
    argv.push_back(bin.data());
    std::vector<std::string> copy = args;
    for (auto& a : copy) argv.push_back(a.data());
    argv.push_back(nullptr);
    ::execv(argv[0], argv.data());
    ::_exit(127);
  }
  ::close(fds[1]);
  std::string out;
  char buf[4096];
  auto deadline = std::chrono::steady_clock::now() + timeout;
  int flags = ::fcntl(fds[0], F_GETFL); ::fcntl(fds[0], F_SETFL, flags | O_NONBLOCK);
  bool eof = false;
  while (!eof) {
    ssize_t n = ::read(fds[0], buf, sizeof(buf));
    if (n > 0) { out.append(buf, size_t(n)); continue; }
    if (n == 0) { eof = true; break; }
    if (std::chrono::steady_clock::now() > deadline) { ::kill(pid, SIGKILL); break; }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  ::close(fds[0]);
  int rc = WaitChild(pid, timeout);
  return { rc, out };
}

std::string Keys(const TempDir& d, std::initializer_list<const char*> names)
{
  std::string s;
  // First queue: 1 GiB storage, size_hint 512 -> 2M ledger slots (~1.08 GiB total).
  // Secondary queues stay at 256 MiB so a pipeline fits in a few GiB of RAM.
  bool first = true;
  for (auto* n : names) {
    s += std::string("[queue ") + n + "]\nkey = " + (d.path / (std::string(n) + ".key")).string()
       + (first ? "\nqueue_size = 1g\nsize_hint = 512" : "\nqueue_size = 256m\nsize_hint = 64") + "\ncreate = true\n\n";
    first = false;
  }
  return s;
}
} // namespace

namespace {
constexpr int64_t kPipelineMemory = kBigQueueBytes + (768LL << 20);   // 1 GiB queue + 256 MiB queue + processes
}

TEST_CASE("Pipeline: two producers -> relay(mask 0b0110) -> sink, via config file", "[pipeline][relay]")
{
  SHMCTX_REQUIRE_MEMORY(kPipelineMemory);
  TempDir d;
  auto cfg = d.Write("fanin.ini", Keys(d, { "md", "strat" }) + R"(
[task feed1]
type = producer
queue = md
id = 1
topic = 1
count = 20000
size = 96

[task feed2]
type = producer
queue = md
id = 2
topic = 2
count = 15000

[task noise]
type = producer
queue = md
id = 3
topic = 3
count = 5000

[task relay]
type = relay
in = md
subscribe = 0b0110
out = strat
out_topic = 5
id = 10
ends = 2

[task sink]
type = consumer
queue = strat
subscribe = 5
ends = 2
)");
  auto r = RunShmTask({ "--config", cfg });
  INFO(r.out);
  CHECK(r.rc == 0);
  using Catch::Matchers::ContainsSubstring;
  CHECK_THAT(r.out, ContainsSubstring("pipeline OK"));
  CHECK_THAT(r.out, ContainsSubstring("relay[relay] ok=1 produced=35000 received=35000 bad=0 misordered=0 ends=2 origins=2"));
  CHECK_THAT(r.out, ContainsSubstring("sink[consumer] ok=1 produced=0 received=35000 bad=0 misordered=0 ends=2 origins=2 max_hops=1"));
  CHECK_THAT(r.out, ContainsSubstring("subscribe=0x6 {1-2}"));
  CHECK_THAT(r.out, ContainsSubstring("overruns=0"));
  CHECK_THAT(r.out, !ContainsSubstring("OVERRUN"));
  CHECK_THAT(r.out, ContainsSubstring("queue md           destroyed rc=0"));
  // Segments were destroyed by the runner.
  for (auto* n : { "md", "strat" }) {
    key_t k = ::ftok((d.path / (std::string(n) + ".key")).c_str(), 8);
    CHECK(::shmget(k, 0, 0) < 0);
  }
}

TEST_CASE("Pipeline: three-hop relay chain on one queue", "[pipeline][relay]")
{
  SHMCTX_REQUIRE_MEMORY(kPipelineMemory);
  TempDir d;
  auto cfg = d.Write("chain.ini", Keys(d, { "bus" }) + R"(
[task src]
type = producer
queue = bus
id = 1
topic = 1
count = 300000

[task hop_a]
type = relay
in = bus
out = bus
subscribe = 1
out_topic = 2
id = 11

[task hop_b]
type = relay
in = bus
out = bus
subscribe = 0x4
out_topic = 3
id = 12

[task sink]
type = consumer
queue = bus
subscribe = 3
)");
  auto r = RunShmTask({ "--config", cfg });
  INFO(r.out);
  CHECK(r.rc == 0);
  using Catch::Matchers::ContainsSubstring;
  CHECK_THAT(r.out, ContainsSubstring("sink[consumer] ok=1 produced=0 received=300000 bad=0 misordered=0 ends=1 origins=1 max_hops=2"));
  CHECK_THAT(r.out, ContainsSubstring("hop_a[relay] ok=1 produced=300000 received=300000"));
  CHECK_THAT(r.out, ContainsSubstring("hop_b[relay] ok=1 produced=300000 received=300000"));
}

TEST_CASE("Pipeline: strategy-id routing with broadcast End markers", "[pipeline][strategy]")
{
  SHMCTX_REQUIRE_MEMORY(kPipelineMemory);
  TempDir d;
  auto cfg = d.Write("strat.ini", Keys(d, { "orders" }) + R"(
[task s7]
type = producer
queue = orders
id = 7
topic = 4
strategy_id = 7
count = 8000

[task s9]
type = producer
queue = orders
id = 9
topic = 4
strategy_id = 9
count = 6000

[task all]
type = consumer
queue = orders
subscribe = 4
strategy_ids = all
ends = 2

[task only7]
type = consumer
queue = orders
subscribe = 4
strategy_ids = 7
ends = 2
expect = 8000
)");
  auto r = RunShmTask({ "--config", cfg });
  INFO(r.out);
  CHECK(r.rc == 0);
  using Catch::Matchers::ContainsSubstring;
  CHECK_THAT(r.out, ContainsSubstring("all[consumer] ok=1 produced=0 received=14000 bad=0 misordered=0 ends=2 origins=2"));
  // only7 stops at expect=8000; whether s9's broadcast End arrived first is timing-dependent (ends=0 or 1).
  CHECK_THAT(r.out, ContainsSubstring("only7[consumer] ok=1 produced=0 received=8000 bad=0 misordered=0 ends="));
  CHECK_THAT(r.out, ContainsSubstring("origins=1 max_hops=0"));
}

TEST_CASE("Pipeline: a sink whose mask misses its producer times out and fails the run", "[pipeline][failure]")
{
  SHMCTX_REQUIRE_MEMORY(kPipelineMemory);
  TempDir d;
  auto cfg = d.Write("miss.ini", Keys(d, { "q" }) + R"(
[task src]
type = producer
queue = q
topic = 1
count = 10

[task sink]
type = consumer
queue = q
subscribe = 2
timeout_s = 1
)");
  auto r = RunShmTask({ "--config", cfg });
  INFO(r.out);
  CHECK(r.rc == 1);
  using Catch::Matchers::ContainsSubstring;
  CHECK_THAT(r.out, ContainsSubstring("TIMEOUT"));
  CHECK_THAT(r.out, ContainsSubstring("pipeline FAILED"));
  CHECK_THAT(r.out, ContainsSubstring("destroyed rc=0"));   // cleanup still happens
}

TEST_CASE("Single tasks from flags only: create -> consumer & relay processes -> producer", "[pipeline][cli]")
{
  SHMCTX_REQUIRE_MEMORY(kPipelineMemory);
  TempDir d;
  std::string a = (d.path / "a.key").string(), b = (d.path / "b.key").string();
  // Create both queues via a tiny config (queue sections only).
  auto cfg = d.Write("queues.ini", Keys(d, { "a", "b" }));
  auto c = RunShmTask({ "--config", cfg, "--create-queues" });
  REQUIRE(c.rc == 0);

  // Start sink and relay as independent processes driven purely by flags; the
  // bit pattern is given on the command line.
  pid_t sink = ForkChild([&] {
    int fd = ::open("/dev/null", O_WRONLY); ::dup2(fd, STDOUT_FILENO); ::dup2(fd, STDERR_FILENO);
    std::string bin = SHM_TASK_BIN;
    ::execl(bin.c_str(), bin.c_str(), "--type", "consumer", "--queue", b.c_str(), "--subscribe", "0b100000", "--ends", "1",
            "--timeout-s", "20", (char*)nullptr);
    return 127;
  });
  pid_t relay = ForkChild([&] {
    int fd = ::open("/dev/null", O_WRONLY); ::dup2(fd, STDOUT_FILENO); ::dup2(fd, STDERR_FILENO);
    std::string bin = SHM_TASK_BIN;
    ::execl(bin.c_str(), bin.c_str(), "--type", "relay", "--in", a.c_str(), "--subscribe", "0x2|3", "--out", b.c_str(),
            "--out-topic", "5", "--id", "42", "--ends", "1", "--timeout-s", "20", (char*)nullptr);
    return 127;
  });
  // Give the two processes time to attach and register their cursors.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  auto p = RunShmTask({ "--type", "producer", "--queue", a, "--id", "1", "--topic", "1", "--count", "12345", "--size", "64" });
  INFO(p.out);
  CHECK(p.rc == 0);
  CHECK_THAT(p.out, Catch::Matchers::ContainsSubstring("cli[producer] ok=1 produced=12345"));
  CHECK(WaitChild(relay, std::chrono::seconds(30)) == 0);
  CHECK(WaitChild(sink, std::chrono::seconds(30)) == 0);

  auto x = RunShmTask({ "--config", cfg, "--destroy-queues" });
  CHECK(x.rc == 0);
}

TEST_CASE("Config task with flag overrides, --list and --dry-run", "[pipeline][cli]")
{
  SHMCTX_REQUIRE_MEMORY(kPipelineMemory);
  TempDir d;
  auto cfg = d.Write("f.ini", Keys(d, { "q" }) + "[task sink]\ntype = consumer\nqueue = q\nsubscribe = 1\n");
  auto l = RunShmTask({ "--config", cfg, "--list" });
  CHECK(l.rc == 0);
  CHECK_THAT(l.out, Catch::Matchers::ContainsSubstring("task  sink [consumer] queue=q subscribe=0x2 {1}"));
  CHECK_THAT(l.out, Catch::Matchers::ContainsSubstring("storage=1 GiB create=1 est_segment=1.08 GiB"));

  auto dry = RunShmTask({ "--config", cfg, "--task", "sink", "--subscribe", "1,2,8-11", "--ends", "3", "--dry-run" });
  CHECK(dry.rc == 0);
  CHECK_THAT(dry.out, Catch::Matchers::ContainsSubstring("subscribe=0xf06 {1-2,8-11} ends=3"));

  auto bad = RunShmTask({ "--config", cfg, "--task", "nope" });
  CHECK(bad.rc == 64);
  CHECK_THAT(bad.out, Catch::Matchers::ContainsSubstring("no [task nope]"));

  auto badmask = RunShmTask({ "--type", "consumer", "--queue", "/x", "--subscribe", "64" });
  CHECK(badmask.rc == 64);
  CHECK_THAT(badmask.out, Catch::Matchers::ContainsSubstring("out of range"));
}
