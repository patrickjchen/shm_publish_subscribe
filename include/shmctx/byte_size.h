#pragma once
// Human-readable byte sizes: "2g", "256M", "64k", "4096", "0x100000", "1.5g".
// Suffixes are binary (k = 1024). Case-insensitive; an optional trailing
// 'b'/'ib' is ignored ("2gb", "2GiB").

#include <cctype>
#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace shmctx {

inline int64_t ParseByteSize(std::string_view text)
{
  std::string s;
  for (char c : text)
    if (!std::isspace(static_cast<unsigned char>(c))) s.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  if (s.empty()) throw std::invalid_argument("empty byte size");

  // strip "b" / "ib"
  if (s.size() > 2 && s.compare(s.size() - 2, 2, "ib") == 0) s.erase(s.size() - 2);
  else if (s.size() > 1 && s.back() == 'b' && !(s.size() > 1 && s[s.size() - 2] == 'x')) s.pop_back();

  int64_t mult = 1;
  switch (s.back()) {
    case 'k': mult = 1LL << 10; break;
    case 'm': mult = 1LL << 20; break;
    case 'g': mult = 1LL << 30; break;
    case 't': mult = 1LL << 40; break;
    default: break;
  }
  if (mult != 1) s.pop_back();
  if (s.empty()) throw std::invalid_argument("byte size has no number: '" + std::string(text) + "'");

  size_t idx = 0;
  long double v;
  try {
    if (s.rfind("0x", 0) == 0) v = static_cast<long double>(std::stoull(s, &idx, 16));
    else v = std::stold(s, &idx);
  } catch (const std::exception&) {
    throw std::invalid_argument("bad byte size '" + std::string(text) + "'");
  }
  if (idx != s.size() || v < 0) throw std::invalid_argument("bad byte size '" + std::string(text) + "'");
  long double bytes = v * static_cast<long double>(mult);
  if (bytes > 9.2e18L) throw std::out_of_range("byte size too large '" + std::string(text) + "'");
  return static_cast<int64_t>(std::llround(bytes));
}

inline std::string FormatByteSize(int64_t bytes)
{
  const char* units[] = { "B", "KiB", "MiB", "GiB", "TiB" };
  int u = 0;
  long double v = static_cast<long double>(bytes);
  while (v >= 1024 && u < 4) { v /= 1024; ++u; }
  char buf[64];
  if (u == 0 || v == std::floor(v)) std::snprintf(buf, sizeof(buf), "%.0Lf %s", v, units[u]);
  else std::snprintf(buf, sizeof(buf), "%.2Lf %s", v, units[u]);
  return buf;
}

} // namespace shmctx
