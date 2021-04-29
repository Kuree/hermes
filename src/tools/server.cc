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

void set_cors(httplib::Response &res) { res.set_header("Access-Control-Allow-Origin", "*"); }

void get_transactions(hermes::Loader &loader, httplib::Server &server) {
    server.Get("/transactions", [&loader](const httplib::Request &req, httplib::Response &res) {
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

        auto start = hermes::parse::parse_uint64(req.get_param_value("start"));
        if (!start) {
            error = set_error("unable to parse start as integer");
        }
        auto end = hermes::parse::parse_uint64(req.get_param_value("end"));
        if (!end) {
            error = set_error("unable to parse end as integer");
        }

        if (!error.empty()) {
            res.set_content(error, json_type);
            res.status = 400;
        } else {
            // need to query and serialize it to json
            auto transactions =
                loader.get_transaction_stream(req.get_param_value("name"), *start, *end);
            // events are raw chunks. we need to actually bound them
            std::string json;
            if (transactions->size() == 0) {
                json = "[]";
            } else {
                auto result =
                    transactions->where([start, end](const hermes::TransactionData &t) -> bool {
                        return t.transaction->start_time() >= *start &&
                               t.transaction->end_time() <= *end;
                    });
                json = result.json();
            }

            res.set_content(json, json_type);
            res.status = 200;
        }
        set_cors(res);
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

    std::cout << "Starting server at " << host_name << ":" << *port << std::endl;
    server->listen(host_name.c_str(), static_cast<int>(*port));

    return EXIT_SUCCESS;
}