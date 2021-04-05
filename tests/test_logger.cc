#include <filesystem>

#include "gtest/gtest.h"
#include "loader.hh"
#include "process.hh"
#include "test_util.hh"
#include "util.hh"

namespace fs = std::filesystem;

fs::path get_root_dir() {
    fs::path current_file = __FILE__;
    return current_file.parent_path().parent_path();
}

fs::path get_build_dir(const fs::path &root) {
    fs::path result;
    for (auto const &dir : fs::directory_iterator(root)) {
        std::string filename = dir.path();
        if (filename.find("build") != std::string::npos) {
            if (result.string().size() < filename.size())
            result = dir.path();
        }
    }
    return result;
}

fs::path get_so(const fs::path &root) {
    auto build = get_build_dir(root);
    return build / "src" / "dpi" / "libhermes-dpi.so";
}

TEST(logger, sv) {  // NOLINT
    // first check if Xcelium is available or not
    auto xrun = hermes::which("xrun");
    if (xrun.empty()) GTEST_SKIP_("xrun not available");

    TempDirectory temp;

    auto root = get_root_dir();
    auto sv_pkg = root / "sv" / "hermes_pkg.sv";
    auto test_logger = root / "tests" / "test_logger.sv";
    EXPECT_TRUE(fs::exists(sv_pkg));
    EXPECT_TRUE(fs::exists(test_logger));

    // need to find build folder to obtain the so file
    auto so = get_so(root);
    EXPECT_TRUE(fs::exists(so));

    const std::vector<std::string> args = {"xrun", sv_pkg, test_logger, "-sv_lib", so};
    auto p = hermes::Process(args, temp.path());
    p.wait();

    // load it back up
    hermes::Loader loader(temp.path());
    auto event_batches = loader.get_events(0, 3000);
    EXPECT_EQ(event_batches.size(), 1);
    auto const &batch = event_batches[0];
    EXPECT_EQ(batch->size(), 2000);

    auto event = (*batch)[42];
    EXPECT_EQ(*event->get_value<uint8_t>("uint8_1"), 42);
    EXPECT_EQ(*event->get_value<std::string>("string_1"), "aaa");

    auto b = event->get_value<bool>("bool");
    EXPECT_TRUE(b);
    EXPECT_FALSE(*b);
    event = (*batch)[43];
    b = event->get_value<bool>("bool");
    EXPECT_TRUE(*b);
}

fs::path get_tracker_lib(const fs::path &root) {
    auto build = get_build_dir(root);
    return build / "tests" / "libtest_tracker_lib.so";
}

TEST(logger, tracker_lib) {  // NOLINT
    // first check if Xcelium is available or not
    auto xrun = hermes::which("xrun");
    if (xrun.empty()) GTEST_SKIP_("xrun not available");

    TempDirectory temp;

    auto root = get_root_dir();
    auto tracker_lib = get_tracker_lib(root);
    EXPECT_TRUE(fs::exists(tracker_lib));
    auto sv_pkg = root / "sv" / "hermes_pkg.sv";
    auto test_tracker_sv = root / "tests" / "test_tracker_lib.sv";
    EXPECT_TRUE(fs::exists(sv_pkg));
    EXPECT_TRUE(fs::exists(test_tracker_sv));
    auto so = get_so(root);
    EXPECT_TRUE(fs::exists(so));

    const std::vector<std::string> args = {"xrun", sv_pkg,    test_tracker_sv, "-sv_lib",
                                           so,     "-sv_lib", tracker_lib};
    auto p = hermes::Process(args, temp.path());
    p.wait();

    // load the transactions_batches
    hermes::Loader loader(temp.path());
    auto transactions_batches = loader.get_transactions(0, 2000);
    auto &batch = transactions_batches[0];
    auto t = (*batch)[0];
    auto events = loader.get_events(*t);
    EXPECT_EQ(events.size(), 10);
    auto v = events[9]->get_value<uint8_t>("uint8_1");
    EXPECT_TRUE(v);
    EXPECT_EQ(*v, 9);
}