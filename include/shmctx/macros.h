#pragma once

#include <cassert>
#include <stdexcept>

namespace mcp::core {

#define MTS_ASSERT(expr, message) assert((expr) && (message))

#define UNREACHABLE(message) throw std::logic_error(message)

// Macros to disable copying and moving
#define DISALLOW_COPY(cname)                 \
  cname(const cname&) = delete; /* NOLINT */ \
  cname& operator=(const cname&) = delete; /* NOLINT */

#define DISALLOW_MOVE(cname)            \
  cname(cname&&) = delete; /* NOLINT */ \
  cname& operator=(cname&&) = delete; /* NOLINT */

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname)                \
  DISALLOW_MOVE(cname)

#if !defined(LIKELY)
#define LIKELY(x) (__builtin_expect(!!(x), 1))
#endif

#if !defined(UNLIKELY)
#define UNLIKELY(x) (__builtin_expect(!!(x), 0))
#endif

#define ALWAYS_INLINE __attribute__((__always_inline__))
#define NO_INLINE __attribute__((__noinline__))
#define MAY_ALIAS __attribute__((__may_alias__))

// for using boost::fiber
#if defined(ADDRESS_SANITIZER)
#define BOOST_USE_ASAN 1
#define BOOST_USE_UCONTEXT 1
#endif

#if defined(THREAD_SANITIZER)
#define BOOST_USE_TSAN 1
#define BOOST_USE_UCONTEXT 1
#endif

// function to suppress warnings about unused variables
template<typename... Args>
constexpr void UNUSED(Args&&... args [[maybe_unused]])
{}

} // namespace mcp::core
