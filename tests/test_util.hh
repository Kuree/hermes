#ifndef HERMES_TEST_UTIL_HH
#define HERMES_TEST_UTIL_HH

#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <cstdio>
#include <filesystem>
#include <random>

#include "../src/loader.hh"
#include "../src/logger.hh"
#include "../src/serializer.hh"

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

std::string get_vector_path(const std::string &filename) {
    auto test_root = fs::path(__FILE__).parent_path();
    return test_root / "vectors" / filename;
}

class EventTransactionInitializer {
public:
    std::shared_ptr<hermes::Loader> loader;
    std::shared_ptr<TempDirectory> temp;
    static constexpr auto name = "test";

    void setup() {
        temp = std::make_shared<TempDirectory>();
        // write out events and transactions
        auto serializer = std::make_shared<hermes::Serializer>(temp->path());
        auto dummy = std::make_shared<hermes::DummyEventSerializer>();
        dummy->connect(serializer);

        auto logger = std::make_shared<hermes::Logger>(name);

        uint64_t time = 0;
        // couple batches
        for (auto b = 0; b < 4; b++) {
            std::shared_ptr<hermes::Transaction> transaction;
            for (auto i = 0; i < 1000; i++) {
                auto event = std::make_shared<hermes::Event>(time++);
                event->add_value<uint64_t>("value", i);
                logger->log(event);
                if (i % 10 == 0) transaction = std::make_shared<hermes::Transaction>();
                transaction->add_event(event);
                if (i % 10 == 9) {
                    transaction->finish();
                    logger->log(transaction);
                }
            }
            // flush it
            dummy->flush();
        }

        hermes::MessageBus::default_bus()->stop();
        serializer->finalize();

        loader = std::make_shared<hermes::Loader>(temp->path());
    }
};

// need to check if a port is open
bool is_port_open(uint16_t port) {
    int fd;
    struct sockaddr_in serv_addr {};
    struct hostent *server;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return false;
    }
    server = gethostbyname("localhost");

    if (server == nullptr) {
        return false;
    }

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    memcpy(server->h_addr, &serv_addr.sin_addr.s_addr, server->h_length);

    serv_addr.sin_port = htons(port);
    bool result = connect(fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) >= 0;
    close(fd);
    return result;
}

#endif  // HERMES_TEST_UTIL_HH
