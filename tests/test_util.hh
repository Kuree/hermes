#ifndef HERMES_TEST_UTIL_HH
#define HERMES_TEST_UTIL_HH

#include <filesystem>
#include <random>

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

#endif  // HERMES_TEST_UTIL_HH
