#include "util.hh"

#ifdef __unix__
#include <unistd.h>
#endif

#ifdef WIN32
#include <windows.h>
#endif

#include <filesystem>

namespace hermes {

std::string which(const std::string &name) {
    // windows is more picky
    std::string env_path;
#ifdef _WIN32
    char *path_var;
    size_t len;
    auto err = _dupenv_s(&path_var, &len, "PATH");
    if (err) {
        env_path = "";
    }
    env_path = std::string(path_var);
    free(path_var);
    path_var = nullptr;
#else
    env_path = std::getenv("PATH");
#endif
    // tokenize it base on either : or ;
    auto tokens = string::split(env_path, ";:");
    for (auto const &dir : tokens) {
        auto new_path = std::filesystem::path(dir) / name;
        if (exists(new_path)) {
            return new_path;
        }
    }
    return "";
}

namespace os {
uint64_t get_total_system_memory() {
    // based on https://stackoverflow.com/a/2513561
#ifdef __unix__
    long pages = sysconf(_SC_PHYS_PAGES);
    long page_size = sysconf(_SC_PAGE_SIZE);
    return pages * page_size;
#endif
#ifdef WIN32
    MEMORYSTATUSEX status;
    status.dwLength = sizeof(status);
    GlobalMemoryStatusEx(&status);
    return status.ullTotalPhys;
#endif
}
}  // namespace os

namespace string {
std::vector<std::string> split(const std::string &str, const std::string &delimiter) {
    std::vector<std::string> tokens;
    size_t prev = 0, pos;
    std::string token;
    // copied from https://stackoverflow.com/a/7621814
    while ((pos = str.find_first_of(delimiter, prev)) != std::string::npos) {
        if (pos > prev) {
            tokens.emplace_back(str.substr(prev, pos - prev));
        }
        prev = pos + 1;
    }
    if (prev < str.length()) tokens.emplace_back(str.substr(prev, std::string::npos));
    // remove empty ones
    std::vector<std::string> result;
    result.reserve(tokens.size());
    for (auto const &t : tokens)
        if (!t.empty()) result.emplace_back(t);
    return result;
}
}  // namespace string

namespace parse {
std::optional<uint64_t> parse_uint64(const std::string &value) {
    try {
        return std::stoull(value);
    } catch (...) {
        return std::nullopt;
    }
}
}  // namespace parse

}  // namespace hermes