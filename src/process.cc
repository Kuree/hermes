#include "process.hh"

#include <filesystem>

#include "subprocess.hpp"

namespace hermes {
Process::Process(const std::vector<std::string> &commands) {
    process_ = std::make_unique<subprocess::Popen>(commands);
}

Process::~Process() { process_->kill(); }

bool PlasmaServer::start() {
#ifdef PLASMA_STORE_SERVER_PATH
#define STRING(s) #s
    if (server_path_.empty()) {
        server_path_ = STRING(PLASMA_STORE_SERVER_PATH);
    }

#undef STRING
#endif
    if (server_path_.empty()) {
        // try to get from environment
        server_path_ = getenv("PLASMA_STORE_SERVER");
    }
    if (server_path_.empty()) return false;

    auto path = std::filesystem::absolute(server_path_);
    auto temp_path = std::filesystem::temp_directory_path() / "plasma";
    // start the server
    auto commands = std::vector<std::string>{server_path_, "-m", std::to_string(mem_size_), "-s",
                                             temp_path.string()};
    server_ = std::make_unique<Process>(commands);
    return true;
}

void PlasmaServer::stop() {
    if (server_) {
        server_.reset();
    }
}

}  // namespace hermes
