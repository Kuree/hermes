#include <filesystem>
#include <fstream>
#include <iostream>

#include "../arrow.hh"
#include "../loader.hh"
#include "arrow/array.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "fmt/format.h"

namespace fs = std::filesystem;

std::string get_filename(const fs::path &path, std::string name) {
    // trying to do some simple escape
    for (auto &c : name) {
        if (c == '/') c = '_';
    }
    return path / fmt::format("{0}.csv", name);
}

void write_table(const std::vector<std::string> &header, std::ofstream &stream,
                 const std::shared_ptr<arrow::Table> &table) {
    auto const &column_chunks = table->column(0);
    for (auto chunk_idx = 0; chunk_idx < column_chunks->num_chunks(); chunk_idx++) {
        // for each row
        auto row_size = column_chunks->chunk(chunk_idx)->length();
        for (int j = 0; j < row_size; j++) {
            std::vector<std::string> row;
            row.reserve(table->num_columns());
            for (int i = 0; i < table->num_columns(); i++) {
                auto const name = header[i];
                auto const &column = table->GetColumnByName(name)->chunk(chunk_idx);
                auto r_ = column->GetScalar(j);
                std::string str;
                if (r_.ok()) {
                    // depends on the type
                    auto const &v = *r_;
                    auto type = v->type;
                    if (type->Equals(arrow::list(arrow::uint64()))) {
                        // list cannot have ','
                        auto list = hermes::get_uint64s(v);
                        str = fmt::format("{0}", fmt::join(list, " "));
                    } else {
                        str = v->ToString();
                    }
                }
                row.emplace_back(str);
            }
            stream << fmt::format("{0}", fmt::join(row, ",")) << std::endl;
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " [log-dir...] output-dir" << std::endl;
        return EXIT_FAILURE;
    }

    std::vector<std::string> paths;
    for (int i = 1; i < argc - 1; i++) {
        paths.emplace_back(argv[i]);
    }

    fs::path output_dir = argv[argc - 1];
    if (!fs::exists(output_dir)) {
        fs::create_directories(output_dir);
    }

    hermes::Loader loader(paths);

    // we only serialize events
    auto const &tables = loader.tables();
    std::unordered_map<std::string, std::unique_ptr<std::ofstream>> writers;
    std::unordered_map<std::string, std::vector<std::string>> headers;
    for (auto const &[info, table] : tables) {
        auto const *file = info.first;
        auto name = file->name;
        if (writers.find(name) == writers.end()) {
            auto filename = get_filename(output_dir, file->name);
            auto stream = std::make_unique<std::ofstream>(filename);
            // write out the headers as well
            auto schema = table->schema();
            *stream << fmt::format("{0}", fmt::join(schema->field_names(), ",")) << std::endl;
            headers.emplace(name, schema->field_names());
            // put it into the writers
            writers.emplace(name, std::move(stream));
        }
        // load the events with raw arrow format
        auto const &fields = headers.at(name);
        auto &stream = *writers.at(name);

        write_table(fields, stream, table);
    }

    for (auto &iter : writers) iter.second->close();
}
