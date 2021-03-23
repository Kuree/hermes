#include <filesystem>
#include <random>

#include "arrow.hh"
#include "event.hh"
#include "serializer.hh"
#include "loader.hh"
#include "gtest/gtest.h"

namespace fs = std::filesystem;

class TempDirectory {
public:
    TempDirectory() {
        auto temp_dir = fs::temp_directory_path();
        std::random_device dev;
        std::mt19937 prng(dev());
        std::uniform_int_distribution<uint64_t> rand(0);
        std::filesystem::path path;

        while (true) {
            std::stringstream ss;
            ss << std::hex << rand(prng);
            path = temp_dir / ss.str();
            // true if the directory was created.
            if (std::filesystem::create_directory(path)) {
                path_ = path;
                break;
            }
        }
    }

    [[nodiscard]] const std::string &path() const { return path_; }

    ~TempDirectory() {
        if (fs::exists(path_)) {
            fs::remove_all(path_);
        }
    }

private:
    std::string path_;
};

TEST(serialization, event) {  // NOLINT
    TempDirectory dir;

    hermes::EventBatch batch;
    constexpr auto num_event = 1000;
    for (auto i = 0; i < num_event; i++) {
        auto e = std::make_unique<hermes::Event>(i);
        e->add_value<uint64_t>("value1", i);
        batch.emplace_back(std::move(e));
    }

    EXPECT_TRUE(batch.validate());
    // serialize it
    hermes::Serializer s(dir.path());
    s.serialize(batch);

    hermes::Loader loader(dir.path());
    auto tables = loader.get_events(0, num_event);
    EXPECT_EQ(tables.size(), 1);

}
