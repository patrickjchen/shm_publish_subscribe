#pragma once
// Parse / format the 64-bit topic subscription mask used by SubscribedTopics.
//
// Accepted spellings (case-insensitive, whitespace ignored):
//   all | *            every topic 0..63
//   none               empty mask
//   0b0110             binary literal, bit i == topic i (LSB = topic 0)
//   0x6                hex literal, same bit meaning
//   1,2,8-11           comma list of topics and inclusive ranges
//   5                  a single topic (NOT a mask: "5" means topic 5)
// Several groups may be joined with '|' or '+': "0x3|8-9|all".

#include <bitset>
#include <cctype>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>

namespace shmctx {

using TopicMask = std::bitset<64>;

namespace detail {
inline std::string Normalize(std::string_view s)
{
  std::string out;
  for (char c : s)
    if (!std::isspace(static_cast<unsigned char>(c))) out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  return out;
}

inline uint64_t ParseUnsigned(std::string_view s, int base, std::string_view what)
{
  if (s.empty()) throw std::invalid_argument(std::string("empty ") + std::string(what));
  uint64_t v = 0;
  for (char c : s) {
    int d;
    if (c >= '0' && c <= '9') d = c - '0';
    else if (c >= 'a' && c <= 'f') d = 10 + (c - 'a');
    else throw std::invalid_argument("bad digit '" + std::string(1, c) + "' in " + std::string(what));
    if (d >= base) throw std::invalid_argument("digit out of range in " + std::string(what));
    if (v > (UINT64_MAX - uint64_t(d)) / uint64_t(base)) throw std::invalid_argument("overflow in " + std::string(what));
    v = v * uint64_t(base) + uint64_t(d);
  }
  return v;
}

inline void ParseGroup(std::string_view g, TopicMask& mask)
{
  if (g == "all" || g == "*") { mask.set(); return; }
  if (g == "none" || g.empty()) return;
  if (g.size() > 2 && g[0] == '0' && g[1] == 'b') { mask |= TopicMask(ParseUnsigned(g.substr(2), 2, "binary mask")); return; }
  if (g.size() > 2 && g[0] == '0' && g[1] == 'x') { mask |= TopicMask(ParseUnsigned(g.substr(2), 16, "hex mask")); return; }

  // comma list of topics / ranges
  size_t pos = 0;
  while (pos <= g.size()) {
    size_t comma = g.find(',', pos);
    std::string_view item = g.substr(pos, comma == std::string_view::npos ? std::string_view::npos : comma - pos);
    if (item.empty()) throw std::invalid_argument("empty item in topic list '" + std::string(g) + "'");
    size_t dash = item.find('-');
    uint64_t lo, hi;
    if (dash == std::string_view::npos) {
      lo = hi = ParseUnsigned(item, 10, "topic");
    } else {
      lo = ParseUnsigned(item.substr(0, dash), 10, "range start");
      hi = ParseUnsigned(item.substr(dash + 1), 10, "range end");
      if (lo > hi) throw std::invalid_argument("descending range '" + std::string(item) + "'");
    }
    if (hi >= mask.size()) throw std::out_of_range("topic " + std::to_string(hi) + " out of range (max 63)");
    for (uint64_t t = lo; t <= hi; ++t) mask.set(t);
    if (comma == std::string_view::npos) break;
    pos = comma + 1;
  }
}
} // namespace detail

inline TopicMask ParseTopicMask(std::string_view text)
{
  std::string s = detail::Normalize(text);
  // FormatTopicMask() appends a "{...}" annotation; accept it back verbatim.
  while (true) {
    size_t open = s.find('{');
    if (open == std::string::npos) break;
    size_t close = s.find('}', open);
    if (close == std::string::npos) throw std::invalid_argument("unterminated '{' in topic mask '" + std::string(text) + "'");
    s.erase(open, close - open + 1);
  }
  TopicMask mask;
  size_t pos = 0;
  while (pos <= s.size()) {
    size_t sep = s.find_first_of("|+", pos);
    std::string_view group(s);
    group = group.substr(pos, sep == std::string::npos ? std::string::npos : sep - pos);
    detail::ParseGroup(group, mask);
    if (sep == std::string::npos) break;
    pos = sep + 1;
  }
  return mask;
}

// Compact hex form plus the topic list, e.g. "0x6 {1,2}" or "0xffffffffffffffff {all}".
inline std::string FormatTopicMask(const TopicMask& mask)
{
  char buf[32];
  std::snprintf(buf, sizeof(buf), "0x%llx", static_cast<unsigned long long>(mask.to_ullong()));
  std::string out = buf;
  if (mask.all()) return out + " {all}";
  if (mask.none()) return out + " {none}";
  out += " {";
  bool first = true;
  for (size_t i = 0; i < mask.size(); ++i) {
    if (!mask.test(i)) continue;
    size_t j = i;
    while (j + 1 < mask.size() && mask.test(j + 1)) ++j;
    if (!first) out += ',';
    first = false;
    out += std::to_string(i);
    if (j > i) out += "-" + std::to_string(j);
    i = j;
  }
  return out + "}";
}

// "1,2,7" -> {1,2,7}; "all" -> {0} (strategy 0 means every strategy in
// SubscribedTopics::IsIncluded); "" / "none" -> {} (broadcast only).
inline std::set<uint16_t> ParseIdList(std::string_view text)
{
  std::string s = detail::Normalize(text);
  std::set<uint16_t> out;
  if (s.empty() || s == "none") return out;
  if (s == "all" || s == "*") { out.insert(0); return out; }
  size_t pos = 0;
  while (pos <= s.size()) {
    size_t comma = s.find(',', pos);
    std::string_view item(s);
    item = item.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
    uint64_t v = detail::ParseUnsigned(item, 10, "id");
    if (v > 0xFFFF) throw std::out_of_range("id " + std::to_string(v) + " exceeds uint16");
    out.insert(static_cast<uint16_t>(v));
    if (comma == std::string::npos) break;
    pos = comma + 1;
  }
  return out;
}

} // namespace shmctx
