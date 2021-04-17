#ifndef HERMES_RTL_HH
#define HERMES_RTL_HH

#include <string>
#include <unordered_map>
#include <vector>

namespace hermes {

struct PackageProxy {
public:
    explicit PackageProxy(const std::unordered_map<std::string, uint64_t> &values)
        : values(values) {}
    std::optional<uint64_t> get(const std::string &name);
    const std::unordered_map<std::string, uint64_t> &values;
};

class RTL {
public:
    explicit RTL(const std::string &filename) : RTL(std::vector<std::string>{filename}) {}
    explicit RTL(const std::vector<std::string> &files) : RTL(files, {}) {}
    RTL(const std::vector<std::string> &files, const std::vector<std::string> &includes);

    [[nodiscard]] bool has_error() const { return !error_message_.empty(); }
    [[nodiscard]] const std::string &error_message() { return error_message_; }

    std::optional<uint64_t> get(const std::string &name);
    std::optional<PackageProxy> package(const std::string &name);

private:
    using EnumDef = std::unordered_map<std::string, uint64_t>;
    // root level enums
    EnumDef enums_;
    std::unordered_map<std::string, EnumDef> package_enums_;
    std::string error_message_;
};
}  // namespace hermes

#endif  // HERMES_RTL_HH
