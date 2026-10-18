// IniFile parser and LoadPipeline / ApplyTaskKey.
#include "shmctx/task.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_exception.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <cstdlib>

using namespace shmctx;

TEST_CASE("INI: sections, kinds, comments, quotes", "[task][ini]")
{
  auto ini = IniFile::ParseString(R"(
# leading comment
[pipeline]
ready_timeout_s = 3

[queue md]            ; trailing comment
key = "/tmp/a b.key"  # quoted value keeps the space
queue_size = 0x100000
create = yes

[task relay]
type = relay
in = md
out = md
subscribe = 0b0110    ; mask
out_topic = 5
name_with_hash = "a#b"
)");
  REQUIRE(ini.Sections().size() == 3);
  auto* pl = ini.Find("", "pipeline");
  REQUIRE(pl);
  CHECK(pl->GetInt("ready_timeout_s", 0) == 3);

  auto* q = ini.Find("queue", "md");
  REQUIRE(q);
  CHECK(q->Get("key") == "/tmp/a b.key");
  CHECK(q->GetInt("queue_size", 0) == 1048576);
  CHECK(q->GetBool("create", false));
  CHECK(q->Get("missing", "dflt") == "dflt");
  CHECK(q->GetInt("missing", 42) == 42);

  auto* t = ini.Find("task", "relay");
  REQUIRE(t);
  CHECK(t->Get("subscribe") == "0b0110");
  CHECK(t->Get("name_with_hash") == "a#b");
  CHECK(ini.SectionsOfKind("task").size() == 1);
  CHECK(ini.Find("task", "nope") == nullptr);
}

TEST_CASE("INI: environment expansion with defaults", "[task][ini]")
{
  ::setenv("SHMCTX_INI_TEST", "/var/x", 1);
  ::unsetenv("SHMCTX_INI_UNSET");
  auto ini = IniFile::ParseString("[queue q]\na = ${SHMCTX_INI_TEST}/k\nb = ${SHMCTX_INI_UNSET:-fallback}\nc = ${SHMCTX_INI_UNSET}end\n");
  auto* q = ini.Find("queue", "q");
  REQUIRE(q);
  CHECK(q->Get("a") == "/var/x/k");
  CHECK(q->Get("b") == "fallback");
  CHECK(q->Get("c") == "end");
}

TEST_CASE("INI: syntax errors carry line numbers", "[task][ini]")
{
  CHECK_THROWS_WITH(IniFile::ParseString("[a\nk=v\n", "f.ini"), Catch::Matchers::ContainsSubstring("f.ini:1"));
  CHECK_THROWS_WITH(IniFile::ParseString("k = v\n", "f.ini"), Catch::Matchers::ContainsSubstring("outside of any section"));
  CHECK_THROWS_WITH(IniFile::ParseString("[s]\nnovalue\n", "f.ini"), Catch::Matchers::ContainsSubstring("f.ini:2"));
  CHECK_THROWS(IniFile::ParseString("[s]\nx = ${UNTERMINATED\n"));
  CHECK_THROWS(IniFile::ParseFile("/nonexistent/path.ini"));
}

TEST_CASE("LoadPipeline builds specs with defaults and validation", "[task][ini][spec]")
{
  auto p = LoadPipeline(IniFile::ParseString(R"(
[queue md]
key = /tmp/md.key
create = true
[queue out]
key = /tmp/out.key
[task src]
type = producer
queue = md
id = 1
topic = 2
count = 10
size = 128
[task relay]
type = relay
in = md
out = out
subscribe = 2
out_topic = 5
strategy_ids = 3,4
ends = 1
[task sink]
type = consumer
queue = out
subscribe = 0x20
check_order = false
timeout_s = 7
)"));
  REQUIRE(p.queues.size() == 2);
  CHECK(p.queues[0].create);
  CHECK_FALSE(p.queues[1].create);
  CHECK(p.queues[0].queue_size == (1 << 20));    // default
  CHECK(p.ResolveKey("md") == "/tmp/md.key");
  CHECK(p.ResolveKey("/other/path") == "/other/path");

  REQUIRE(p.tasks.size() == 3);
  auto& src = p.tasks[0];
  CHECK(src.type == TaskType::Producer);
  CHECK(src.topic == 2);
  CHECK(src.count == 10);
  CHECK(src.size == 128);

  auto& relay = p.tasks[1];
  CHECK(relay.type == TaskType::Relay);
  CHECK(relay.subscribe.to_ullong() == 0x4);
  CHECK(relay.out_topic == 5);
  CHECK(relay.strategy_ids == std::set<uint16_t>{ 3, 4 });

  auto& sink = p.tasks[2];
  CHECK(sink.subscribe.to_ullong() == 0x20);
  CHECK_FALSE(sink.check_order);
  CHECK(sink.timeout_s == 7);
  CHECK(sink.ends == 1);                           // default
  CHECK(p.FindTask("sink") == &sink);
}

TEST_CASE("LoadPipeline rejects inconsistent specs", "[task][ini][spec]")
{
  CHECK_THROWS_WITH(LoadPipeline(IniFile::ParseString("[queue q]\ncreate=true\n")), Catch::Matchers::ContainsSubstring("needs 'key'"));
  CHECK_THROWS_WITH(LoadPipeline(IniFile::ParseString("[task t]\nqueue=q\n")), Catch::Matchers::ContainsSubstring("needs 'type'"));
  CHECK_THROWS_WITH(LoadPipeline(IniFile::ParseString("[task t]\ntype=producer\n")), Catch::Matchers::ContainsSubstring("needs 'queue'"));
  CHECK_THROWS_WITH(LoadPipeline(IniFile::ParseString("[task t]\ntype=relay\nin=a\n")), Catch::Matchers::ContainsSubstring("'in' and 'out'"));
  CHECK_THROWS_WITH(LoadPipeline(IniFile::ParseString("[task t]\ntype=relay\nin=a\nout=a\nsubscribe=all\nout_topic=3\n")),
                    Catch::Matchers::ContainsSubstring("loop forever"));
  CHECK_THROWS_WITH(LoadPipeline(IniFile::ParseString("[task t]\ntype=consumer\nqueue=q\nsubscribe=none\n")), Catch::Matchers::ContainsSubstring("empty"));
  CHECK_THROWS_WITH(LoadPipeline(IniFile::ParseString("[task t]\ntype=producer\nqueue=q\nsize=8\n")), Catch::Matchers::ContainsSubstring("size must be"));
  CHECK_THROWS_WITH(LoadPipeline(IniFile::ParseString("[task t]\ntype=producer\nqueue=q\nbogus=1\n")), Catch::Matchers::ContainsSubstring("unknown task key"));
  CHECK_THROWS_WITH(LoadPipeline(IniFile::ParseString("[task t]\ntype=producer\nqueue=q\n[task t]\ntype=producer\nqueue=q\n")), Catch::Matchers::ContainsSubstring("duplicate task"));
  CHECK_THROWS_WITH(LoadPipeline(IniFile::ParseString("[task t]\ntype=producer\nqueue=q\ntopic=64\n")), Catch::Matchers::ContainsSubstring("topic"));
}

TEST_CASE("ApplyTaskKey accepts CLI spelling and overrides file values", "[task][spec]")
{
  TaskSpec t;
  ApplyTaskKey(t, "type", "relay");
  ApplyTaskKey(t, "in", "/tmp/a.key");
  ApplyTaskKey(t, "out", "/tmp/b.key");
  ApplyTaskKey(t, "out-topic", "9");          // dash form
  ApplyTaskKey(t, "subscribe", "0x3|5");
  ApplyTaskKey(t, "strategy-ids", "all");
  ApplyTaskKey(t, "check_order", "off");
  CHECK(t.type == TaskType::Relay);
  CHECK(t.out_topic == 9);
  CHECK(t.subscribe.to_ullong() == 0x23);
  CHECK(t.strategy_ids == std::set<uint16_t>{ 0 });
  CHECK_FALSE(t.check_order);
  ApplyTaskKey(t, "subscribe", "7");          // override
  CHECK(t.subscribe.to_ullong() == 0x80);
  CHECK_THROWS(ApplyTaskKey(t, "topic", "-1"));
  CHECK_THROWS(ApplyTaskKey(t, "type", "sink"));
  CHECK_NOTHROW(ValidateTask(t));
  CHECK(DescribeTask(t).find("out_topic=9") != std::string::npos);
}
