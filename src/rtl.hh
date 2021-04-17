#ifndef HERMES_RTL_HH
#define HERMES_RTL_HH

#include <string>
#include <unordered_map>
#include <vector>

namespace hermes {

// enum def name, value>
using EnumDef = std::pair<std::string, uint64_t>;
using EnumMap = std::unordered_map<std::string, EnumDef>;

struct PackageProxy {
public:
    explicit PackageProxy(const EnumMap &values);
    [[nodiscard]] std::optional<uint64_t> get(const std::string &name) const;
    const EnumMap &values;

    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> index;
};

class RTL {
public:
    explicit RTL(const std::string &filename) : RTL(std::vector<std::string>{filename}) {}
    explicit RTL(const std::vector<std::string> &files) : RTL(files, {}) {}
    RTL(const std::vector<std::string> &files, const std::vector<std::string> &includes);

    [[nodiscard]] bool has_error() const { return !error_message_.empty(); }
    [[nodiscard]] const std::string &error_message() const { return error_message_; }

    std::optional<uint64_t> get(const std::string &name) const;
    std::optional<PackageProxy> package(const std::string &name) const;
    [[nodiscard]] bool has_package(const std::string &name) const {
        return package_enums_.find(name) != package_enums_.end();
    }

    std::optional<std::string> lookup(uint64_t value);
    std::optional<std::string> lookup(uint64_t value, const std::string &enum_name);
    std::optional<std::string> lookup(uint64_t value, const std::string &pkg_name,
                                      const std::string &enum_name);
    // compilation unit
    [[nodiscard]] const EnumMap &get_root_enums() const { return enums_; }

private:
    // root level enums
    EnumMap enums_;
    // package level enums
    std::unordered_map<std::string, EnumMap> package_enums_;

    // indicate if there is any error when parsing the RTL
    std::string error_message_;

    static std::optional<std::string> lookup(uint64_t value, const std::string &enum_def_name,
                                             const EnumMap &enum_map);
};
}  // namespace hermes

#endif  // HERMES_RTL_HH
