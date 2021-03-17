#include "plasma.hh"
#include "fmt/format.h"
#include "plasma/client.h"
#include "plasma/test_util.h"

#include <filesystem>

namespace hermes {

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
    auto temp_path = pipe_filename_.empty()
                         ? std::string(std::filesystem::temp_directory_path() / "plasma")
                         : pipe_filename_;
    // start the server
    auto commands =
        std::vector<std::string>{server_path_, "-m", std::to_string(mem_size_), "-s", temp_path};
    server_ = std::make_unique<Process>(commands);
    return true;
}

void PlasmaServer::stop() {
    if (server_) {
        server_.reset();
    }
}

void send_event_batch(const EventBatch &batch, plasma::PlasmaClient *client) {
    auto obj_id = plasma::random_object_id();

}


}  // namespace hermes
