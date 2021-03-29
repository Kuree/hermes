#include <filesystem>

#include "event.hh"
#include "gtest/gtest.h"
#include "loader.hh"
#include "process.hh"
#include "util.hh"

namespace fs = std::filesystem;

fs::path get_root_dir() {
    fs::path current_file = __FILE__;
    return current_file.parent_path().parent_path();
}

fs::path get_so(const fs::path &root) {
    for (auto const &dir : fs::directory_iterator(root)) {
        std::string filename = dir.path();
        if (filename.find("build") != std::string::npos) {
            return dir.path() / "src" / "dpi" / "libhermes-dpi.so";
        }
    }
    return "";
}

TEST(logger, sv) {  // NOLINT
    // first check if Xcelium is available or not
    auto xrun = hermes::which("xrun");
    if (xrun.empty()) GTEST_SKIP_("xrun not available");

    auto root = get_root_dir();
    auto sv_pkg = root / "sv" / "hermes_pkg.sv";
    auto test_logger = root / "tests" / "test_logger.sv";
    EXPECT_TRUE(fs::exists(sv_pkg));
    EXPECT_TRUE(fs::exists(test_logger));

    // need to find build folder to obtain the so file
    auto so = get_so(root);
    EXPECT_TRUE(fs::exists(so));

    const std::vector<std::string> args = {"xrun", sv_pkg, test_logger, "-sv_lib", so};
    auto p = hermes::Process(args);
    p.wait();
}
