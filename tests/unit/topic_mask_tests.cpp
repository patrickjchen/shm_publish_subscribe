// ParseTopicMask / FormatTopicMask / ParseIdList.
#include "shmctx/byte_size.h"
#include "shmctx/task.h"
#include "shmctx/topic_mask.h"

#include <catch2/catch_test_macros.hpp>

using namespace shmctx;

TEST_CASE("Topic mask: binary and hex literals map bit i to topic i", "[task][mask]")
{
  CHECK(ParseTopicMask("0b0110").to_ullong() == 0x6);
  CHECK(ParseTopicMask("0B0110").to_ullong() == 0x6);
  CHECK(ParseTopicMask("0x6").to_ullong() == 0x6);
  CHECK(ParseTopicMask("0xFFFFFFFFFFFFFFFF").all());
  CHECK(ParseTopicMask("0b1").test(0));
  CHECK(ParseTopicMask("0b1000000000000000000000000000000000000000000000000000000000000000").test(63));
}

TEST_CASE("Topic mask: lists, ranges, keywords, groups", "[task][mask]")
{
  CHECK(ParseTopicMask("5").to_ullong() == (1ULL << 5));           // a number is a topic, not a mask
  CHECK(ParseTopicMask("1,2,8-11").to_ullong() == 0b111100000110ULL);
  CHECK(ParseTopicMask(" 1 , 2 ").to_ullong() == 0x6);
  CHECK(ParseTopicMask("all").all());
  CHECK(ParseTopicMask("*").all());
  CHECK(ParseTopicMask("none").none());
  CHECK(ParseTopicMask("").none());
  CHECK(ParseTopicMask("0x3|8-9").to_ullong() == 0x303);
  CHECK(ParseTopicMask("1+0b100").to_ullong() == 0x6);
  CHECK(ParseTopicMask("63").test(63));
}

TEST_CASE("Topic mask: rejects malformed input", "[task][mask]")
{
  CHECK_THROWS_AS(ParseTopicMask("64"), std::out_of_range);
  CHECK_THROWS_AS(ParseTopicMask("1-70"), std::out_of_range);
  CHECK_THROWS_AS(ParseTopicMask("5-3"), std::invalid_argument);
  CHECK_THROWS_AS(ParseTopicMask("0b102"), std::invalid_argument);
  CHECK_THROWS_AS(ParseTopicMask("0xg"), std::invalid_argument);
  CHECK_THROWS_AS(ParseTopicMask("1,,2"), std::invalid_argument);
  CHECK_THROWS_AS(ParseTopicMask("abc"), std::invalid_argument);
  CHECK_THROWS_AS(ParseTopicMask("0x10000000000000000"), std::invalid_argument);   // 65 bits
}

TEST_CASE("Topic mask formatting is compact and round-trips", "[task][mask]")
{
  CHECK(FormatTopicMask(ParseTopicMask("0b0110")) == "0x6 {1-2}");
  CHECK(FormatTopicMask(ParseTopicMask("1,3,8-11")) == "0xf0a {1,3,8-11}");
  CHECK(FormatTopicMask(ParseTopicMask("all")) == "0xffffffffffffffff {all}");
  CHECK(FormatTopicMask(ParseTopicMask("none")) == "0x0 {none}");
  for (const char* s : { "0x6", "1,3,8-11", "63", "0-63", "0,62" })
    CHECK(ParseTopicMask(FormatTopicMask(ParseTopicMask(s))) == ParseTopicMask(s));
}

TEST_CASE("Strategy id lists", "[task][mask]")
{
  CHECK(ParseIdList("") == std::set<uint16_t>{});
  CHECK(ParseIdList("none") == std::set<uint16_t>{});
  CHECK(ParseIdList("all") == std::set<uint16_t>{ 0 });
  CHECK(ParseIdList("7") == std::set<uint16_t>{ 7 });
  CHECK(ParseIdList("7, 9,7") == std::set<uint16_t>{ 7, 9 });
  CHECK_THROWS_AS(ParseIdList("70000"), std::out_of_range);
  CHECK_THROWS_AS(ParseIdList("x"), std::invalid_argument);
}

TEST_CASE("Byte sizes: suffixes, hex, fractions, formatting", "[task][bytes]")
{
  CHECK(ParseByteSize("4096") == 4096);
  CHECK(ParseByteSize("64k") == 64 * 1024);
  CHECK(ParseByteSize("256M") == 256LL << 20);
  CHECK(ParseByteSize("2g") == 2LL << 30);
  CHECK(ParseByteSize("2GiB") == 2LL << 30);
  CHECK(ParseByteSize("2gb") == 2LL << 30);
  CHECK(ParseByteSize("1.5g") == 3LL << 29);
  CHECK(ParseByteSize("0x100000") == 1 << 20);
  CHECK(ParseByteSize(" 8 m ") == 8LL << 20);
  CHECK_THROWS_AS(ParseByteSize(""), std::invalid_argument);
  CHECK_THROWS_AS(ParseByteSize("g"), std::invalid_argument);
  CHECK_THROWS_AS(ParseByteSize("12x"), std::invalid_argument);
  CHECK_THROWS_AS(ParseByteSize("-1"), std::invalid_argument);
  CHECK_THROWS_AS(ParseByteSize("99999999t"), std::out_of_range);
  CHECK(FormatByteSize(2LL << 30) == "2 GiB");
  CHECK(FormatByteSize(1536) == "1.50 KiB");
  CHECK(FormatByteSize(100) == "100 B");
}

TEST_CASE("Queue memory estimate matches the layout formula", "[task][bytes]")
{
  QueueSpec q; q.queue_size = 2LL << 30; q.size_hint = 512; q.consumers = 8;
  // storage 2 GiB + 4M ledger slots * 40 B = 160 MiB + 8*64 + header
  CHECK(EstimateQueueBytes(q) == (2LL << 30) + (4LL << 20) * 40 + 8 * 64 + 256);
  q.size_hint = 64;
  CHECK(EstimateQueueBytes(q) > (3LL << 30));          // the 64-byte hint makes it ~3.5 GiB
  QueueSpec small; small.queue_size = 1000; small.size_hint = 60; small.consumers = 1;
  CHECK(EstimateQueueBytes(small) == 1024 + 16 * 40 + 64 + 256);
}

TEST_CASE("Queue spec keys accept human-readable sizes", "[task][bytes][spec]")
{
  QueueSpec q;
  ApplyQueueKey(q, "queue_size", "2g");
  ApplyQueueKey(q, "size_hint", "512");
  CHECK(q.queue_size == 2LL << 30);
  CHECK(q.size_hint == 512);
  TaskSpec t;
  ApplyTaskKey(t, "size", "1k");
  CHECK(t.size == 1024);
}
