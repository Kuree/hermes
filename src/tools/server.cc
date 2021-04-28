#define CPPHTTPLIB_OPENSSL_SUPPORT

#include <iostream>
#include <memory>

#include "../json.hh"
#include "../loader.hh"
#include "../util.hh"
#include "httplib.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

constexpr auto json_type = "application/json";

int print_help(const std::string &app) {
    std::cerr << "Usage " << app << " hostname:port [logs...]" << std::endl;
    return EXIT_FAILURE;
}

std::string set_error(const std::string &reason) {
    rapidjson::Document doc(rapidjson::kObjectType);
    hermes::json::set_member(doc, "reason", reason);
    return hermes::json::serialize(doc, false);
}

void get_transactions(hermes::Loader &loader, httplib::Server &server) {
    server.Get("/transactions",
               [&loader, &server](const httplib::Request &req, httplib::Response &res) {
                   // make sure all the parameters are correct
                   std::string error;
                   if (!req.has_param("name")) {
                       error = set_error("name not set");
                   }
                   if (error.empty() && !req.has_param("start")) {
                       error = set_error("start not set");
                   }
                   if (error.empty() && !req.has_param("end")) {
                       error = set_error("end not set");
                   }

                   if (!error.empty()) {
                       res.set_content(error, json_type);
                   } else {
                       // need to query and se
                   }
               });
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        return print_help(argv[0]);
    }
    // parse the host and port
    std::string name_ports = argv[1];
    std::vector<std::string> paths;
    paths.reserve(argc - 2);
    for (int i = 2; i < argc; i++) {
        paths.emplace_back(argv[i]);
    }
    auto values = hermes::string::split(name_ports, ":");
    if (values.size() != 2) {
        return print_help(argv[0]);
    }
    std::string host_name = values[0];
    std::string port_value = values[1];
    auto port = hermes::parse::parse_uint64(port_value);
    if (!port) {
        return print_help(argv[0]);
    }

    auto loader = hermes::Loader(paths);

    auto server = std::make_unique<httplib::Server>();
    get_transactions(loader, *server);

    server->listen(host_name.c_str(), static_cast<int>(*port));

    return EXIT_SUCCESS;
}