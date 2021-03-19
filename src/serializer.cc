#include <filesystem>
#include "serializer.hh"

namespace fs = std::filesystem;

namespace hermes {

Serializer::Serializer(std::string output_dir): output_dir_(std::move(output_dir)) {
    if (!fs::exists(output_dir_)) {
        fs::create_directories(output_dir_);
    }
}

}
