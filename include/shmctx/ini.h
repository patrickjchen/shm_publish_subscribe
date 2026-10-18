#pragma once
// Minimal INI reader for pipeline configs. No dependencies.
//
//   # comment            ; comment
//   [task relay]         section header: "<kind> <name>", or just "<name>"
//   key = value          values may be quoted; ${VAR} / ${VAR:-default} expand from the environment
//
// Sections keep file order. Duplicate keys: last wins.

#include <cctype>
#include <cstdlib>
#include <fstream>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace shmctx {

struct IniSection
{
  std::string kind;   // "task", "queue", "pipeline", ... ("" if header had one token)
  std::string name;
  std::map<std::string, std::string> values;
  int line = 0;

  bool Has(const std::string& k) const { return values.count(k) != 0; }
  std::string Get(const std::string& k, const std::string& def = "") const
  {
    auto it = values.find(k);
    return it == values.end() ? def : it->second;
  }
  long long GetInt(const std::string& k, long long def) const
  {
    auto it = values.find(k);
    if (it == values.end()) return def;
    size_t idx = 0;
    long long v = std::stoll(it->second, &idx, 0);
    if (idx != it->second.size()) throw std::invalid_argument("[" + kind + " " + name + "] " + k + ": not an integer: " + it->second);
    return v;
  }
  bool GetBool(const std::string& k, bool def) const
  {
    auto it = values.find(k);
    if (it == values.end()) return def;
    std::string v;
    for (char c : it->second) v.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    if (v == "1" || v == "true" || v == "yes" || v == "on") return true;
    if (v == "0" || v == "false" || v == "no" || v == "off") return false;
    throw std::invalid_argument("[" + kind + " " + name + "] " + k + ": not a boolean: " + it->second);
  }
};

class IniFile
{
public:
  static IniFile Parse(std::istream& in, const std::string& source = "<stream>")
  {
    IniFile ini;
    ini.source_ = source;
    std::string line;
    int lineno = 0;
    IniSection* cur = nullptr;
    while (std::getline(in, line)) {
      ++lineno;
      std::string_view sv = Strip(StripComment(line));
      if (sv.empty()) continue;
      if (sv.front() == '[') {
        if (sv.back() != ']') throw std::runtime_error(source + ":" + std::to_string(lineno) + ": unterminated section header");
        std::string_view inner = Strip(sv.substr(1, sv.size() - 2));
        IniSection sec;
        sec.line = lineno;
        size_t sp = inner.find_first_of(" \t");
        if (sp == std::string_view::npos) {
          sec.name = std::string(inner);
        } else {
          sec.kind = std::string(inner.substr(0, sp));
          sec.name = std::string(Strip(inner.substr(sp + 1)));
        }
        if (sec.name.empty()) throw std::runtime_error(source + ":" + std::to_string(lineno) + ": empty section name");
        ini.sections_.push_back(std::move(sec));
        cur = &ini.sections_.back();
        continue;
      }
      size_t eq = sv.find('=');
      if (eq == std::string_view::npos) throw std::runtime_error(source + ":" + std::to_string(lineno) + ": expected key = value");
      if (!cur) throw std::runtime_error(source + ":" + std::to_string(lineno) + ": key outside of any section");
      std::string key(Strip(sv.substr(0, eq)));
      std::string val = Unquote(Strip(sv.substr(eq + 1)));
      if (key.empty()) throw std::runtime_error(source + ":" + std::to_string(lineno) + ": empty key");
      cur->values[key] = ExpandEnv(val);
    }
    return ini;
  }

  static IniFile ParseFile(const std::string& path)
  {
    std::ifstream f(path);
    if (!f) throw std::runtime_error("cannot open config file: " + path);
    return Parse(f, path);
  }

  static IniFile ParseString(const std::string& text, const std::string& source = "<string>")
  {
    std::istringstream s(text);
    return Parse(s, source);
  }

  const std::vector<IniSection>& Sections() const { return sections_; }

  std::vector<const IniSection*> SectionsOfKind(std::string_view kind) const
  {
    std::vector<const IniSection*> out;
    for (auto& s : sections_)
      if (s.kind == kind) out.push_back(&s);
    return out;
  }

  const IniSection* Find(std::string_view kind, std::string_view name) const
  {
    for (auto& s : sections_)
      if (s.kind == kind && s.name == name) return &s;
    return nullptr;
  }

  const std::string& Source() const { return source_; }

private:
  static std::string_view Strip(std::string_view s)
  {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) s.remove_prefix(1);
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) s.remove_suffix(1);
    return s;
  }
  // Drop "# ..." / "; ..." unless inside quotes.
  static std::string_view StripComment(std::string_view s)
  {
    bool quoted = false;
    for (size_t i = 0; i < s.size(); ++i) {
      if (s[i] == '"') quoted = !quoted;
      else if (!quoted && (s[i] == '#' || s[i] == ';')) return s.substr(0, i);
    }
    return s;
  }
  static std::string Unquote(std::string_view s)
  {
    if (s.size() >= 2 && s.front() == '"' && s.back() == '"') return std::string(s.substr(1, s.size() - 2));
    return std::string(s);
  }
  static std::string ExpandEnv(const std::string& v)
  {
    std::string out;
    for (size_t i = 0; i < v.size();) {
      if (v[i] == '$' && i + 1 < v.size() && v[i + 1] == '{') {
        size_t close = v.find('}', i + 2);
        if (close == std::string::npos) throw std::runtime_error("unterminated ${ in value: " + v);
        std::string expr = v.substr(i + 2, close - i - 2);
        std::string name = expr, def;
        bool has_def = false;
        if (size_t d = expr.find(":-"); d != std::string::npos) { name = expr.substr(0, d); def = expr.substr(d + 2); has_def = true; }
        const char* env = std::getenv(name.c_str());
        if (env && *env) out += env;
        else if (has_def) out += def;
        i = close + 1;
      } else {
        out.push_back(v[i++]);
      }
    }
    return out;
  }

  std::string source_;
  std::vector<IniSection> sections_;
};

} // namespace shmctx
