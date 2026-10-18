#pragma once

#include <atomic>
#include <bitset>
#include <set>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <limits>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <iostream>
#include "shmctx/shm_queue_types.h"
#include "shmctx/spin_lock.h"

namespace mcp::core {


class ShmProducer;
class ShmConsumer;
class ShmAdmin;

struct SubscribedTopics
{
  std::bitset<64> topics;
  std::set<uint16_t> strategy_ids;

  void set()
  {
    topics.set();
  }

  void set(size_t bit, bool value = true)
  {
    topics.set(bit, value);
  }

  void reset()
  {
    topics.reset();
  }

  void reset(size_t bit)
  {
    topics.reset(bit);
  }

  void set_strategy_ids(const std::set<uint16_t>& strat_ids)
  {
    strategy_ids = strat_ids;  
  }

  void set_strategy_ids(uint16_t strat_id)
  {
    strategy_ids.insert(strat_id); 
  }

  bool IsIncluded(int strat_id) const
  {
    return (0 == strat_id || (!strategy_ids.empty() && strategy_ids.count(0) != 0)  || strategy_ids.count(strat_id) !=0);
  }
 
};


// Consumer Stats
struct ConsumerStats
{
  int64_t sequence_num;
  pid_t consumer_pid;
  bool occupied;
};

// TODO: Return the produced sequence number from the Produce function
// TODO: Have consumers handle evictions gracefully

// Multiproducer, Multiconsumer
class ShmContext
{

  struct Header
  {
    alignas(64) std::atomic_int64_t sequence_num;
    alignas(64) std::atomic_int64_t claimed_sequence_num;
    alignas(64) std::atomic_int64_t current_offset_;
    alignas(64) AtomicFlagSpinlock reader_index_lock;
    int64_t LEDGER_SIZE;
    int64_t STORAGE_SIZE;
    int64_t CONSUMERS;
    std::atomic_bool initialized_ = false;
  };

  struct Metadata
  {
    int64_t offset;
    int64_t size;
    uint16_t topic;
    uint16_t strategy_id;
    // Claim stamp written by the producer immediately after it CAS-bumps
    // claimed_sequence_num. Used by other producers to detect that the
    // claimer died before committing (see TryRecoverDeadProducer).
    // claim_seq matches claimed_desired so recoverers can distinguish a
    // current claim from stale data left by a previous wrap of this slot.
    int64_t claim_seq;
    std::atomic<pid_t> claimer_pid;
  };
  static_assert(std::atomic<pid_t>::is_always_lock_free,
                "claimer_pid must be lock-free for cross-process recovery");

  struct alignas(64) ConsumerSequence
  {
    std::atomic_int64_t sequence_num = 0;
    std::atomic_int64_t offset = 0;
    pid_t pid = 0;
    std::atomic_bool done = false;
    std::atomic_bool occupied = false;
  };

public:
  explicit ShmContext(std::string name, bool shared_memory = false)
    : name_(name),
      memory_size_(0),
      header_(nullptr),
      reader_sequence_numbers_(nullptr),
      ledger_(nullptr),
      storage_(nullptr),
      shared_memory_(shared_memory)
  {}

  ~ShmContext()
  {
    if (queue_ptr_ == nullptr) return;

    if (!shared_memory_) {
      munmap(queue_ptr_, memory_size_);
    } else {
      // detach from shared memory
      shmdt(queue_ptr_);
    }
  }

  int Create(int64_t queue_size, int64_t num_consumers, int64_t size_hint)
  {

    auto actual_queue_size = GetNextPowerOfTwo(queue_size);
    int64_t actual_ledger_size = actual_queue_size / GetNextPowerOfTwo(size_hint);

    int64_t total_size = sizeof(Header) + (sizeof(ConsumerSequence) * num_consumers)
                         + (actual_ledger_size * sizeof(Metadata)) + actual_queue_size;

    if (!shared_memory_) {
      // mmap enough space for the header, the queue, the ledger and the consumer sequence numbers
      queue_ptr_ = mmap(NULL, total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
      memory_size_ = total_size;
    } else {
      // attach from shared memory
      key_t shm_key = ftok(name_.c_str(), 8);
      auto shm_id = shmget(shm_key, total_size, IPC_CREAT | 0666);
      if (shm_id < 0) {
        perror("shmget");
        return -1;
      }
      queue_ptr_ = shmat(shm_id, NULL, 0);
    }

    // place into memory
    memset(queue_ptr_, 0, total_size);

    header_ = reinterpret_cast<Header*>(queue_ptr_);
    header_->LEDGER_SIZE = actual_ledger_size;
    header_->STORAGE_SIZE = actual_queue_size;
    header_->CONSUMERS = num_consumers;

    reader_sequence_numbers_ = reinterpret_cast<ConsumerSequence*>(header_ + 1);
    ledger_ = reinterpret_cast<Metadata*>(reader_sequence_numbers_ + num_consumers);
    storage_ = reinterpret_cast<char*>(ledger_ + actual_ledger_size);

    header_->initialized_.store(true);
    return 0;
  }

  int Attach()
  {
    // attach only works with shared memory
    if (!shared_memory_) { return -1; }

    key_t shm_key = ftok(name_.c_str(), 8);
    auto shm_id = shmget(shm_key, 0, 0);
    if (shm_id < 0) {
      perror("shmget");
      return -1;
    }
    queue_ptr_ = shmat(shm_id, NULL, 0);
    if (queue_ptr_ == MAP_FAILED) {
      perror("shmat");
      return -1;
    }

    header_ = reinterpret_cast<Header*>(queue_ptr_);
    while (!header_->initialized_) {}
    reader_sequence_numbers_ = reinterpret_cast<ConsumerSequence*>(header_ + 1);
    ledger_ = reinterpret_cast<Metadata*>(reader_sequence_numbers_ + header_->CONSUMERS);
    storage_ = reinterpret_cast<char*>(ledger_ + header_->LEDGER_SIZE);
    return 0;
  }

  int Destroy()
  {
    if (!shared_memory_) { return 0; }
    key_t shm_key = ftok(name_.c_str(), 8);
    auto shmid = shmget(shm_key, 0, 0);
    if (shmid < 0) { perror("shmget"); }
    return shmctl(shmid, IPC_RMID, NULL);
  }

  // destroy

  int64_t GetLedgerSize() const { return header_->LEDGER_SIZE; }

  int64_t GetStorageSize() const { return header_->STORAGE_SIZE; }

  int64_t GetConsumers() const { return header_->CONSUMERS; }

  int64_t GetCursor() const {
    return header_->sequence_num.load(std::memory_order_relaxed);
  }

  const char* const GetStorage() const { return storage_; }

private:

  /////////////////////////////////////////////////////////////////////////////
  // Crash recovery helpers
  //
  // The producer protocol is a two-phase commit:
  //   1. CAS-bump claimed_sequence_num   (claim slot N)
  //   2. spin until sequence_num == N-1  (wait for our turn)
  //   3. write payload + sequence_num.store(N)  (commit)
  // If a producer dies between (1) and (3), every later producer waiting in
  // (2) and every consumer in Consume() blocks forever. To unwedge the queue,
  // each claimer stamps its pid + claim_seq into the slot right after (1);
  // other producers stuck in (2) periodically check liveness of the claimer
  // ahead of them and, on death, mark the slot as SKIP and CAS-advance
  // sequence_num past it.
  /////////////////////////////////////////////////////////////////////////////

  // Phase-2 timeouts. These guard the recovery slow-path; the fast-path is
  // unchanged. Sized so legitimately slow producers (page faults, NUMA
  // hiccups) don't get evicted, while crashed producers don't wedge the queue.
  static constexpr int64_t LIVENESS_PROBE_NS = 100'000'000;       // 100 ms
  static constexpr int64_t FORCE_SKIP_NS     = 10'000'000'000;    // 10 s

  // Mark a slot as SKIP and CAS-advance sequence_num. Returns true iff this
  // call was the one that advanced sequence_num (losers see the value moved
  // by another recoverer or by the original producer's late commit).
  bool MarkSlotSkippedAndAdvance(int64_t target_seq, int64_t expected_committed, int64_t idx)
  {
    ledger_[idx].size = 0;
    ledger_[idx].offset = 0;
    ledger_[idx].topic = kSkipTopicSentinel;
    ledger_[idx].strategy_id = 0;
    int64_t expected_local = expected_committed;
    return header_->sequence_num.compare_exchange_strong(
      expected_local, target_seq, std::memory_order_acq_rel);
  }

  // Tier 1 recovery: if the producer holding the slot just ahead of the
  // committed head has a stamped pid and the kernel says that pid no longer
  // exists, skip its slot. Returns true on a successful CAS-advance.
  // Caveat: relies on Linux pid not being reused before we probe. For HFT
  // run lengths this is acceptable; can be hardened later by also stamping
  // /proc/<pid>/stat starttime.
  bool TryRecoverDeadProducer()
  {
    int64_t current = header_->sequence_num.load(std::memory_order_acquire);
    int64_t target_seq = current + 1;
    if (header_->claimed_sequence_num.load(std::memory_order_acquire) < target_seq) {
      return false;  // nothing claimed past current head
    }
    int64_t idx = target_seq & (header_->LEDGER_SIZE - 1);
    pid_t holder = ledger_[idx].claimer_pid.load(std::memory_order_acquire);
    int64_t holder_seq = ledger_[idx].claim_seq;
    if (holder == 0 || holder_seq != target_seq) {
      // Either the claimer is still in the CAS-vs-stamp window, or this slot
      // shows stale data from a previous wrap. Wait for the FORCE_SKIP path.
      return false;
    }
    if (holder == getpid()) return false;  // self - shouldn't happen
    if (kill(holder, 0) == 0) return false;  // alive
    if (errno != ESRCH) return false;  // permission error etc., not death
    return MarkSlotSkippedAndAdvance(target_seq, current, idx);
  }

  // Tier 2 recovery: closes the gap between CAS and the pid stamp. If
  // someone has claimed slot target_seq but no commit has landed for ages,
  // force-skip regardless of pid stamp.
  bool ForceSkipDeadProducer()
  {
    int64_t current = header_->sequence_num.load(std::memory_order_acquire);
    int64_t target_seq = current + 1;
    if (header_->claimed_sequence_num.load(std::memory_order_acquire) < target_seq) {
      return false;  // nothing was actually claimed
    }
    int64_t idx = target_seq & (header_->LEDGER_SIZE - 1);
    return MarkSlotSkippedAndAdvance(target_seq, current, idx);
  }

  // Replacement for the inline `while (sequence_num != expected) spin/yield`
  // loop. Invokes Tier-1 then Tier-2 recovery if waiting drags on. Multiple
  // dead slots ahead are recovered one at a time across loop iterations.
  void WaitForCommit(int64_t expected_committed)
  {
    using clock = std::chrono::steady_clock;
    int64_t count = 0;
    auto stuck_start = clock::time_point::min();
    bool tier1_attempted = false;
    while (header_->sequence_num.load(std::memory_order_acquire) != expected_committed) {
      ++count;
      if (count >= NUM_ITER) [[unlikely]] {
        std::this_thread::yield();
        count = 0;
        auto now = clock::now();
        if (stuck_start == clock::time_point::min()) {
          stuck_start = now;
          continue;
        }
        auto elapsed_ns =
          std::chrono::duration_cast<std::chrono::nanoseconds>(now - stuck_start).count();
        if (elapsed_ns >= FORCE_SKIP_NS) {
          if (ForceSkipDeadProducer()) {
            // Slot ahead skipped; reset timers and re-evaluate.
            stuck_start = clock::time_point::min();
            tier1_attempted = false;
          } else {
            // Reset window so we don't hammer.
            stuck_start = now;
          }
        } else if (!tier1_attempted && elapsed_ns >= LIVENESS_PROBE_NS) {
          tier1_attempted = true;
          if (TryRecoverDeadProducer()) {
            stuck_start = clock::time_point::min();
            tier1_attempted = false;
          }
        }
      }
    }
  }

  // Stamp the per-slot owner immediately after CAS-claiming. Must publish
  // claim_seq before claimer_pid so a recoverer that reads pid (acquire)
  // can trust claim_seq.
  void StampClaim(int64_t claimed_desired)
  {
    int64_t idx = claimed_desired & (header_->LEDGER_SIZE - 1);
    ledger_[idx].claim_seq = claimed_desired;
    ledger_[idx].claimer_pid.store(getpid(), std::memory_order_release);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Producer Functions
  /////////////////////////////////////////////////////////////////////////////

  int Produce(const char* data, int64_t size, uint16_t topic = 0, uint16_t strategy_id = 0)
  {

    // get the next sequence number
    auto claimed_expected = header_->claimed_sequence_num.load(std::memory_order_acquire);
    auto claimed_desired = claimed_expected + 1;
    const auto LEDGER_SIZE = header_->LEDGER_SIZE;
    const auto STORAGE_SIZE = header_->STORAGE_SIZE;

    while (!header_->claimed_sequence_num.compare_exchange_weak(
      claimed_expected, claimed_desired, std::memory_order_acq_rel)) {
      claimed_desired = claimed_expected + 1;
    }
    StampClaim(claimed_desired);

    // wait for the sequence number to catch up
    int64_t expected = claimed_desired - 1;
    WaitForCommit(expected);

    auto curr_offset = header_->current_offset_.load(std::memory_order_acquire);
    auto remaining_size = STORAGE_SIZE - (curr_offset & STORAGE_SIZE - 1) - 1;
    int64_t offset_start = 0, offset_end = 0;

    if (size > remaining_size) [[unlikely]] {
      offset_start = curr_offset + remaining_size + 1;
      offset_end = offset_start + size - 1;
    } else {
      offset_start = curr_offset + 1;
      offset_end = offset_start + size - 1;
    }

    ledger_[claimed_desired & LEDGER_SIZE - 1].offset = offset_start;
    ledger_[claimed_desired & LEDGER_SIZE - 1].size = size;
    ledger_[claimed_desired & LEDGER_SIZE - 1].topic = topic;
    ledger_[claimed_desired & LEDGER_SIZE - 1].strategy_id = strategy_id;

    std::memcpy(&storage_[offset_start & STORAGE_SIZE - 1], data, size);

    header_->current_offset_.store(offset_end, std::memory_order_relaxed);
    header_->sequence_num.store(claimed_desired, std::memory_order_release);
    return 0;
  }

  int ProduceWithSeqNumAndTimestamp(char* data,
                                    int64_t size,
                                    size_t seq_offset,
                                    size_t timestamp_offset,
                                    uint16_t topic = 0,
                                    uint16_t strategy_id = 0)
  {

    using namespace std::chrono;
    // get the next sequence number
    auto claimed_expected = header_->claimed_sequence_num.load(std::memory_order_acquire);
    auto claimed_desired = claimed_expected + 1;
    const auto LEDGER_SIZE = header_->LEDGER_SIZE;
    const auto STORAGE_SIZE = header_->STORAGE_SIZE;

    while (!header_->claimed_sequence_num.compare_exchange_weak(
      claimed_expected, claimed_desired, std::memory_order_acq_rel)) {
      claimed_desired = claimed_expected + 1;
    }
    StampClaim(claimed_desired);

    // wait for the sequence number to catch up
    int64_t expected = claimed_desired - 1;
    *(reinterpret_cast<uint64_t*>(data + seq_offset)) = claimed_desired;
    WaitForCommit(expected);

    // check if we can write
    auto curr_offset = header_->current_offset_.load(std::memory_order_acquire);
    auto remaining_size = STORAGE_SIZE - (curr_offset & STORAGE_SIZE - 1) - 1;
    int64_t offset_start = 0, offset_end = 0;

    if (size > remaining_size) [[unlikely]] {
      offset_start = curr_offset + remaining_size + 1;
      offset_end = offset_start + size - 1;
    } else {
      offset_start = curr_offset + 1;
      offset_end = offset_start + size - 1;
    }

    ledger_[claimed_desired & LEDGER_SIZE - 1].offset = offset_start;
    ledger_[claimed_desired & LEDGER_SIZE - 1].size = size;
    ledger_[claimed_desired & LEDGER_SIZE - 1].topic = topic;
    ledger_[claimed_desired & LEDGER_SIZE - 1].strategy_id = strategy_id;

    *(reinterpret_cast<int64_t*>(data + timestamp_offset)) =
      duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
    std::memcpy(&storage_[offset_start & STORAGE_SIZE - 1], data, size);

    header_->current_offset_.store(offset_end, std::memory_order_relaxed);
    header_->sequence_num.store(claimed_desired, std::memory_order_release);
    return 0;
  }

  int Produce(const std::vector<BufferWrapper>& batch, uint16_t topic = 0, uint16_t strategy_id = 0)
  {
    int64_t size = 0;
    for (const auto& packet : batch) { size += packet.size; }
    // get the next sequence number
    auto claimed_expected = header_->claimed_sequence_num.load(std::memory_order_acquire);
    auto claimed_desired = claimed_expected + 1;
    const auto LEDGER_SIZE = header_->LEDGER_SIZE;
    const auto STORAGE_SIZE = header_->STORAGE_SIZE;

    while (!header_->claimed_sequence_num.compare_exchange_weak(
      claimed_expected, claimed_desired, std::memory_order_acq_rel)) {
      claimed_desired = claimed_expected + 1;
    }
    StampClaim(claimed_desired);

    // wait for the sequence number to catch up
    int64_t expected = claimed_desired - 1;
    WaitForCommit(expected);

    // check if we can write
    auto curr_offset = header_->current_offset_.load(std::memory_order_acquire);
    auto remaining_size = STORAGE_SIZE - (curr_offset & STORAGE_SIZE - 1) - 1;
    int64_t offset_start = 0, offset_end = 0;

    if (size > remaining_size) [[unlikely]] {
      offset_start = curr_offset + remaining_size + 1;
      offset_end = offset_start + size - 1;
    } else {
      offset_start = curr_offset + 1;
      offset_end = offset_start + size - 1;
    }

    ledger_[claimed_desired & LEDGER_SIZE - 1].offset = offset_start;
    ledger_[claimed_desired & LEDGER_SIZE - 1].size = size;
    ledger_[claimed_desired & LEDGER_SIZE - 1].topic = topic;
    ledger_[claimed_desired & LEDGER_SIZE - 1].strategy_id = strategy_id;

    auto curr_idx = offset_start & STORAGE_SIZE - 1;
    for (const auto& packet : batch) {
      std::memcpy(storage_ + curr_idx, packet.data, packet.size);
      curr_idx += packet.size;
    }
    header_->current_offset_.store(offset_end, std::memory_order_relaxed);
    header_->sequence_num.store(claimed_desired, std::memory_order_release);
    return 0;
  }

  /**
   * IF using this API, please make sure that the seq_offset is given from batch[0]
   */
  int ProduceWithSeqNumAndTimestamp(std::vector<BufferWrapper>& batch,
                                    size_t seq_offset,
                                    size_t timestamp_offset,
                                    uint16_t topic = 0,
                                    uint16_t strategy_id = 0)
  {
    using namespace std::chrono;
    int64_t size = 0;
    for (const auto& packet : batch) { size += packet.size; }
    // get the next sequence number
    auto claimed_expected = header_->claimed_sequence_num.load(std::memory_order_acquire);
    auto claimed_desired = claimed_expected + 1;
    const auto LEDGER_SIZE = header_->LEDGER_SIZE;
    const auto STORAGE_SIZE = header_->STORAGE_SIZE;

    while (!header_->claimed_sequence_num.compare_exchange_weak(
      claimed_expected, claimed_desired, std::memory_order_acq_rel)) {
      claimed_desired = claimed_expected + 1;
    }
    StampClaim(claimed_desired);

    // write the sequence number in (a little bit hacky)
    *(reinterpret_cast<uint64_t*>(batch[0].data + seq_offset)) = claimed_desired;


    // wait for the sequence number to catch up
    int64_t expected = claimed_desired - 1;
    WaitForCommit(expected);

    // check if we can write
    auto curr_offset = header_->current_offset_.load(std::memory_order_acquire);
    auto remaining_size = STORAGE_SIZE - (curr_offset & STORAGE_SIZE - 1) - 1;
    int64_t offset_start = 0, offset_end = 0;

    if (size > remaining_size) [[unlikely]] {
      offset_start = curr_offset + remaining_size + 1;
      offset_end = offset_start + size - 1;
    } else {
      offset_start = curr_offset + 1;
      offset_end = offset_start + size - 1;
    }

    ledger_[claimed_desired & LEDGER_SIZE - 1].offset = offset_start;
    ledger_[claimed_desired & LEDGER_SIZE - 1].size = size;
    ledger_[claimed_desired & LEDGER_SIZE - 1].topic = topic;
    ledger_[claimed_desired & LEDGER_SIZE - 1].strategy_id = strategy_id;

    auto curr_idx = offset_start & STORAGE_SIZE - 1;
    *(reinterpret_cast<int64_t*>(batch[0].data + timestamp_offset)) =
      duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();

    for (const auto& packet : batch) {
      std::memcpy(storage_ + curr_idx, packet.data, packet.size);
      curr_idx += packet.size;
    }

    header_->current_offset_.store(offset_end, std::memory_order_relaxed);
    header_->sequence_num.store(claimed_desired, std::memory_order_release);
    return 0;
  }


  /////////////////////////////////////////////////////////////////////////////
  // Consumer Functions
  //
  // Consume()
  // TryConsume()
  // Consume(topic)
  /////////////////////////////////////////////////////////////////////////////

  // Consume
  BufferWrapper Consume(int64_t consumer_sequence)
  {
    while (header_->sequence_num.load(std::memory_order_acquire) < consumer_sequence) {}
    int64_t idx = consumer_sequence & (header_->LEDGER_SIZE - 1);
    auto offset = ledger_[idx].offset;
    auto size = ledger_[idx].size;
    auto topic = ledger_[idx].topic;
    auto strategy_id = ledger_[idx].strategy_id;

    auto storage_idx = offset & (header_->STORAGE_SIZE - 1);

    return BufferWrapper{ storage_ + storage_idx, size, topic, strategy_id };
  }

  // Returns -1 size if cannot be consumed
  BufferWrapper TryConsume(int64_t consumer_sequence)
  {
    if (header_->sequence_num.load(std::memory_order_acquire) < consumer_sequence) {
      return BufferWrapper{ nullptr, -1, 0, 0 };
    }

    int64_t idx = consumer_sequence & (header_->LEDGER_SIZE - 1);
    auto offset = ledger_[idx].offset;
    auto size = ledger_[idx].size;
    auto topic = ledger_[idx].topic;
    auto strategy_id = ledger_[idx].strategy_id;

    auto storage_idx = offset & (header_->STORAGE_SIZE - 1);

    return BufferWrapper{ storage_ + storage_idx, size, topic, strategy_id };
  }

  // Try Consume with topic
  std::pair<BufferWrapper, int64_t> TryConsume(int64_t consumer_sequence, const SubscribedTopics& topics)
  {
    auto global_seq_no = header_->sequence_num.load(std::memory_order_acquire);

    if (global_seq_no < consumer_sequence) { return std::make_pair(BufferWrapper{ nullptr, -1, 0, 0 }, consumer_sequence); }

    // iterate until we find a topic that matches the subscription list or we catch upto head
    while (consumer_sequence <= global_seq_no) {
      int64_t idx = consumer_sequence & (header_->LEDGER_SIZE - 1);
      auto topic = ledger_[idx].topic;
      auto strategy_id = ledger_[idx].strategy_id;

      // Slots abandoned by a crashed producer (recovered by another producer)
      // are tagged with kSkipTopicSentinel and must not be dispatched. Also
      // guards against std::bitset<64>::test(0xFFFF) being out of range.
      if (topic == kSkipTopicSentinel) {
        consumer_sequence++;
        continue;
      }

      if (topics.topics.test(topic) && topics.IsIncluded(strategy_id)) {
        // the topic exists
        auto offset = ledger_[idx].offset;
        auto size = ledger_[idx].size;
        return std::make_pair(BufferWrapper{ storage_ + (offset & header_->STORAGE_SIZE - 1), size, topic, strategy_id }, consumer_sequence + 1);
      }
      consumer_sequence++;
    }

    return std::make_pair(BufferWrapper{ nullptr, -1, 0, 0 }, consumer_sequence);
  }


  /////////////////////////////////////////////////////////////////////////////
  // Admin Functions
  //
  // GetMinConsumerSequencer()
  // GetConsumerSequenceLedger
  // GetCurrentSequenceNumber()
  // GetClaimedSequenceNumber()
  /////////////////////////////////////////////////////////////////////////////

  int64_t GetMinConsumerSequence()
  {
    int64_t output = std::numeric_limits<int64_t>::max();
    for (int i = 0; i < header_->CONSUMERS; ++i) {
      if (reader_sequence_numbers_[i].occupied) {
        output = std::min(output, reader_sequence_numbers_[i].sequence_num.load(std::memory_order_relaxed));
      }
    }
    return output;
  }

  void GetConsumerStats(std::vector<ConsumerStats>& output)
  {
    if (output.size() != header_->CONSUMERS) { output.resize(header_->CONSUMERS); }
    for (int i = 0; i < header_->CONSUMERS; ++i) {
      output[i].occupied = reader_sequence_numbers_[i].occupied.load(std::memory_order_relaxed);
      output[i].sequence_num = reader_sequence_numbers_[i].sequence_num.load(std::memory_order_relaxed);
      output[i].consumer_pid = reader_sequence_numbers_[i].pid;
    }
  }

  int64_t GetCurrentSequenceNumber() { return header_->sequence_num.load(std::memory_order_relaxed); }

  int64_t GetClaimedSequenceNumber() { return header_->claimed_sequence_num.load(std::memory_order_relaxed); }

  static int64_t GetNextPowerOfTwo(int64_t size)
  {
    int64_t i{ 0 };
    if (size > std::numeric_limits<int64_t>::max() / 2) { return -1; }
    for (; (1L << i) < size; ++i)
      ;
    return 1L << i;
  }

  std::string name_;
  uint64_t memory_size_;
  void* queue_ptr_ = nullptr;
  Header* header_;
  ConsumerSequence* reader_sequence_numbers_;
  Metadata* ledger_;
  char* storage_;
  bool shared_memory_ = false;

  static constexpr int64_t NUM_ITER = 1024;

  friend class ShmProducer;
  friend class ShmConsumer;
  friend class ShmAdmin;
#ifdef SHMCTX_ENABLE_TEST_HOOKS
  // Test-only access used by the integration suite to simulate a producer
  // that dies between claim and commit. Never defined in production builds.
  friend struct ShmContextTestHooks;
#endif
};


class ShmProducer
{

public:
  explicit ShmProducer(ShmContext& context)
    : context_(context)
  {}

  DISALLOW_COPY(ShmProducer)


  /**
   * PRODUCE
   */
  int Produce(const char* data, const int64_t size) { return context_.Produce(data, size); }

  int Produce(const std::vector<BufferWrapper>& batch) { return context_.Produce(batch); }

  template<typename T>
  int Produce(const T& obj)
  {
    return context_.Produce(reinterpret_cast<const char*>(&obj), sizeof(T));
  }


  /**
   * PRODUCE WITH TOPIC
   */
  template<typename T>
  int ProduceWithTopic(const T& obj, const uint16_t topic)
  {
    return context_.Produce(reinterpret_cast<const char*>(&obj), sizeof(T), topic);
  }

  int ProduceWithTopic(const char* data, const int64_t size, const uint16_t topic)
  {
    return context_.Produce(data, size, topic);
  }

  int ProduceWithTopic(const std::vector<BufferWrapper>& batch, const uint16_t topic)
  {
    return context_.Produce(batch, topic);
  }

  /**
   * PRODUCE WITH SEQ NUM AND TIMESTAMP
   */
  int ProduceWithSeqAndTimestamp(char* data, const int64_t size, const size_t seq_offset, const size_t time_offset)
  {
    return context_.ProduceWithSeqNumAndTimestamp(data, size, seq_offset, time_offset);
  }

  template<typename T>
  int ProduceWithSeqAndTimestamp(T& obj, const size_t seq_offset, const size_t time_offset)
  {
    return context_.ProduceWithSeqNumAndTimestamp(reinterpret_cast<char*>(&obj), sizeof(T), seq_offset, time_offset);
  }

  int ProduceWithSeqAndTimestamp(std::vector<BufferWrapper>& batch, size_t seq_offset, size_t time_offset)
  {
    return context_.ProduceWithSeqNumAndTimestamp(batch, seq_offset, time_offset);
  }

  /**
   * PRODUCE WITH SEQ NUM, TIMESTAMP, TOPIC (NON CONST ARG)
   */

  template<typename T>
  int ProduceWithSeqAndTimestampAndTopic(T& obj,
                                         const size_t seq_offset,
                                         const size_t time_offset,
                                         const uint16_t topic)
  {
    return context_.ProduceWithSeqNumAndTimestamp(
      reinterpret_cast<char*>(&obj), sizeof(T), seq_offset, time_offset, topic);
  }

  int ProduceWithSeqAndTimestampAndTopic(char* data,
                                         const int64_t size,
                                         const size_t seq_offset,
                                         const size_t time_offset,
                                         const uint16_t topic)
  {
    return context_.ProduceWithSeqNumAndTimestamp(data, size, seq_offset, time_offset, topic);
  }

  int ProduceWithSeqAndTimestampAndTopic(std::vector<BufferWrapper>& batch,
                                         const size_t seq_offset,
                                         const size_t time_offset,
                                         const uint16_t topic)
  {
    return context_.ProduceWithSeqNumAndTimestamp(batch, seq_offset, time_offset, topic);
  }

  /**
   * PRODUCE WITH SEQ NUM, TIMESTAMP, TOPIC (NON CONST ARG) AND STATEGY_ID
   */

  template<typename T>
  int Produce(T& obj,
              const size_t seq_offset,
              const size_t time_offset,
              const uint16_t topic,
              const uint16_t strategy_id)
  {
    return context_.ProduceWithSeqNumAndTimestamp(
      reinterpret_cast<char*>(&obj), sizeof(T), seq_offset, time_offset, topic, strategy_id);
  }

  int Produce(char* data,
              const int64_t size,
              const size_t seq_offset,
              const size_t time_offset,
              const uint16_t topic,
              const uint16_t strategy_id)
  {
    return context_.ProduceWithSeqNumAndTimestamp(data, size, seq_offset, time_offset, topic, strategy_id);
  }

  int Produce(std::vector<BufferWrapper>& batch,
              const size_t seq_offset,
              const size_t time_offset,
              const uint16_t topic,
              const uint16_t strategy_id)
  {
    return context_.ProduceWithSeqNumAndTimestamp(batch, seq_offset, time_offset, topic, strategy_id);
  }

private:
  ShmContext& context_;
};

/**
 * Packet Producer : keeps the offsets of the sequence number and timestamps cached
 *
 */

class PacketProducer
{

public:
  PacketProducer(ShmContext& context, size_t sequence_offset, size_t timestamp_offset)
    : producer_(context),
      sequence_offset_(sequence_offset),
      timestamp_offset_(timestamp_offset)
  {}

  /**
   * PRODUCE
   */
  int Produce(char* data, const int64_t size)
  {
    return producer_.ProduceWithSeqAndTimestamp(data, size, sequence_offset_, timestamp_offset_);
  }

  int Produce(std::vector<BufferWrapper>& batch)
  {
    return producer_.ProduceWithSeqAndTimestamp(batch, sequence_offset_, timestamp_offset_);
  }

  template<typename T>
  int Produce(const T& obj)
  {
    return producer_.ProduceWithSeqAndTimestamp(obj, sequence_offset_, timestamp_offset_);
  }


  /**
   * PRODUCE WITH TOPIC
   */
  template<typename T>
  int ProduceWithTopic(T& obj, const uint16_t topic)
  {
    return producer_.ProduceWithSeqAndTimestampAndTopic(obj, sequence_offset_, timestamp_offset_, topic);
  }

  int ProduceWithTopic(char* data, const int64_t size, const uint16_t topic)
  {
    return producer_.ProduceWithSeqAndTimestampAndTopic(data, size, sequence_offset_, timestamp_offset_, topic);
  }

  int ProduceWithTopic(std::vector<BufferWrapper>& batch, const uint16_t topic)
  {
    return producer_.ProduceWithSeqAndTimestampAndTopic(batch, sequence_offset_, timestamp_offset_, topic);
  }

/**
   * PRODUCE WITH TOPIC AND STRATEGY_ID
   */
  template<typename T>
  int ProduceWithTopicAndStrategyId(T& obj, const uint16_t topic, const uint16_t strategy_id)
  {
    return producer_.Produce(obj, sequence_offset_, timestamp_offset_, topic, strategy_id);
  }

  int ProduceWithTopicAndStrategyId(char* data, const int64_t size, const uint16_t topic, const uint16_t strategy_id)
  {
    return producer_.Produce(data, size, sequence_offset_, timestamp_offset_, topic, strategy_id);
  }

  int ProduceWithTopicAndStrategyId(std::vector<BufferWrapper>& batch, const uint16_t topic, const uint16_t strategy_id)
  {
    return producer_.Produce(batch, sequence_offset_, timestamp_offset_, topic, strategy_id);
  }

private:
  ShmProducer producer_;
  size_t sequence_offset_;
  size_t timestamp_offset_;
};

/**
 * BatchProducer
 */
class BatchProducer
{

public:
  BatchProducer(ShmContext& context, int64_t batch_size)
    : producer_(context)
  {
    if (batch_size <= 0) return;
    batch_ = new char[batch_size];
  }

  ~BatchProducer()
  {
    if (batch_ != nullptr) { delete[] batch_; }
  }

  DISALLOW_COPY(BatchProducer)

  template<typename T>
  T* GetNextMessage()
  {
    auto out = reinterpret_cast<T*>(batch_ + offset_);
    offset_ += sizeof(T);
    return out;
  }

  int SendBatch()
  {
    auto out = producer_.Produce(batch_, offset_);
    if (!out) return out;
    // clear
    offset_ = 0;
    return out;
  }

  // send batch with a topic
  // TODO: implement
  int SendBatch(int64_t topic)
  {
    auto out = producer_.Produce(batch_, offset_);
    if (!out) return out;
    // clear
    offset_ = 0;
    return out;
  }

  int64_t GetSize() { return offset_; }

  void Clear() { offset_ = 0; }

private:
  ShmProducer producer_;
  char* batch_ = nullptr;
  int64_t offset_ = 0;
};

/**
 * Shm Consumer
 */
class ShmConsumer
{

public:
  explicit ShmConsumer(ShmContext& context)
    : context_(context)
  {
    consumer_idx_ = context.GetCursor() + 1;
  }

  ~ShmConsumer() = default;

  DISALLOW_COPY(ShmConsumer)

  bool Registered() const { return (consumer_idx_ != -1); }

  int GetConsumerID() const { return consumer_idx_; }

  inline BufferWrapper Consume() {
    // Slots abandoned by a crashed producer carry kSkipTopicSentinel; the
    // commit-recovery path on the producer side advances sequence_num past
    // them, so we just need to skip and re-fetch.
    while (true) {
      auto out = context_.Consume(consumer_idx_);
      consumer_idx_++;
      if (out.topic != kSkipTopicSentinel) return out;
    }
  }

  inline BufferWrapper TryConsume() {
    while (true) {
      auto out = context_.TryConsume(consumer_idx_);
      if (out.data == nullptr) return out;  // nothing available
      consumer_idx_++;
      if (out.topic != kSkipTopicSentinel) return out;
    }
  }

  inline BufferWrapper TryConsume(const SubscribedTopics& topic_list)
  {
    auto out = context_.TryConsume(consumer_idx_, topic_list);
    consumer_idx_ = out.second;
    return out.first;
  }

  inline std::pair<int64_t, int64_t> GetCursor() const
  {
    return {consumer_idx_, context_.GetCursor()};
  }

  void Release() { /*noop*/ }

  // call this before publishing the DD

private:
  ShmContext& context_;
  int64_t consumer_idx_ = -1;
  bool deferred_init_ = false;
};

/**
 * ShmAdmin : reads the state of all consumers and the current sequence numbers.
 * Checks if consumers are falling behind, and can possibly evict them if there's an issue
 * TODO: if a producer dies after calling Produce but before committing the SequenceNum, we'll have deadlock. We should
 * think of a solution to this
 */
class ShmAdmin
{

public:
  explicit ShmAdmin(ShmContext& context)
    : context_(context)
  {}

  DISALLOW_COPY_AND_MOVE(ShmAdmin)

  void GetConsumerStats(std::vector<ConsumerStats>& output) { context_.GetConsumerStats(output); }

  int64_t GetCurrentSequenceNumber() { return context_.GetCurrentSequenceNumber(); }

  int64_t GetClaimedSequenceNumber() { return context_.GetClaimedSequenceNumber(); }

  void DeregisterConsumer(int) {}

  // add support to evict customers

private:
  ShmContext& context_;
};


} // namespace mcp::core
