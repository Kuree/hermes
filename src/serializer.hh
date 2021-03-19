#ifndef HERMES_SERIALIZER_HH
#define HERMES_SERIALIZER_HH

#include <string>

namespace hermes {
class Serializer {
public:
    explicit Serializer(std::string output_dir);

private:
    std::string output_dir_;
};
}

#endif  // HERMES_SERIALIZER_HH
