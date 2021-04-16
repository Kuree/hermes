#ifndef HERMES_RTL_HH
#define HERMES_RTL_HH

#include <string>
#include <unordered_map>
#include <vector>

namespace hermes {

struct Enum {
    std::string name;
    std::unordered_map<std::string, uint64_t> values;
};

class RTL {
public:
    explicit RTL(const std::string &filename) : RTL(std::vector<std::string>{filename}) {}
    explicit RTL(const std::vector<std::string> &files) : RTL(files, {}) {}
    RTL(const std::vector<std::string> &files, const std::vector<std::string> &includes);

    [[nodiscard]] bool has_error() const { return !error_message_.empty(); }
    [[nodiscard]] const std::string &error_message() { return error_message_; }

private:
    using EnumDef = std::unordered_map<std::string, Enum>;
    // root level enums
    EnumDef enums_;
    std::unordered_map<std::string, EnumDef> package_enums_;
    std::string error_message_;
};
}  // namespace hermes

#endif  // HERMES_RTL_HH
