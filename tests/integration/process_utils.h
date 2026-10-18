#pragma once
// fork()/pipe helpers for the multi-process integration tests.
//
// Children must never run Catch2 assertions or return normally into the test
// runner; they report via exit code and finish with _exit().

#include <chrono>
#include <csignal>
#include <cstdio>
#include <functional>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace shmctx_test {

// Fork and run `body` in the child; its return value becomes the exit code.
inline pid_t ForkChild(const std::function<int()>& body)
{
  pid_t pid = ::fork();
  if (pid < 0) { std::perror("fork"); std::abort(); }
  if (pid == 0) {
    int rc = 1;
    try { rc = body(); } catch (...) { rc = 99; }
    std::fflush(nullptr);
    ::_exit(rc);
  }
  return pid;
}

// Wait for `pid` up to `timeout`. Returns the exit code, or -1 on timeout
// (in which case the child is killed) and -2 if it died from a signal.
inline int WaitChild(pid_t pid, std::chrono::milliseconds timeout = std::chrono::seconds(30))
{
  auto deadline = std::chrono::steady_clock::now() + timeout;
  int status = 0;
  while (true) {
    pid_t r = ::waitpid(pid, &status, WNOHANG);
    if (r == pid) break;
    if (r < 0) { std::perror("waitpid"); return -3; }
    if (std::chrono::steady_clock::now() > deadline) {
      ::kill(pid, SIGKILL);
      ::waitpid(pid, &status, 0);
      return -1;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  if (WIFEXITED(status)) return WEXITSTATUS(status);
  return -2;
}

// One-byte "ready" signal from child to parent.
class ReadyPipe
{
public:
  ReadyPipe() { if (::pipe(fds_) != 0) { std::perror("pipe"); std::abort(); } }
  ~ReadyPipe() { CloseRead(); CloseWrite(); }

  void SignalReady()
  {
    char c = 'R';
    (void)!::write(fds_[1], &c, 1);
    CloseWrite();
  }
  bool WaitReady(std::chrono::milliseconds timeout = std::chrono::seconds(10))
  {
    CloseWrite();   // parent side: only read
    fd_set set; FD_ZERO(&set); FD_SET(fds_[0], &set);
    timeval tv{ static_cast<time_t>(timeout.count() / 1000), static_cast<suseconds_t>((timeout.count() % 1000) * 1000) };
    int r = ::select(fds_[0] + 1, &set, nullptr, nullptr, &tv);
    if (r <= 0) return false;
    char c = 0;
    return ::read(fds_[0], &c, 1) == 1 && c == 'R';
  }

private:
  void CloseRead() { if (fds_[0] >= 0) { ::close(fds_[0]); fds_[0] = -1; } }
  void CloseWrite() { if (fds_[1] >= 0) { ::close(fds_[1]); fds_[1] = -1; } }
  int fds_[2]{ -1, -1 };
};

} // namespace shmctx_test
