#include <filesystem>
#include "serializer.hh"
#include

namespace fs = std::filesystem;

namespace hermes {

Serializer::Serializer(std::string output_dir): output_dir_(std::move(output_dir)) {
    if (!fs::exists(output_dir_)) {
        fs::create_directories(output_dir_);
    }
}

void Serializer::serialize(const EventBatch &batch) {

}

void Serializer::serialize(const TransactionBatch &batch) {

}

}
