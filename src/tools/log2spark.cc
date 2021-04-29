// nothing really going on, just convert some schema to the format that
// apache spark understands
// Arrow itself might have support this if this is actually gets implemented
// https://issues.apache.org/jira/browse/ARROW-1988
// we convert schema for the following things
// 1. unsigned integers -> signed integers
// 2. arrays are dropped

#include <filesystem>
#include <fstream>
#include <iostream>

#include "../arrow.hh"
#include "../loader.hh"
#include "arrow/api.h"
#include "arrow/array.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/table.h"
#include "parquet/arrow/writer.h"
#include "parquet/metadata.h"

namespace fs = std::filesystem;

std::shared_ptr<arrow::Schema> rewrite_schema(const std::shared_ptr<arrow::Schema> &schema) {
    std::vector<std::shared_ptr<arrow::Field>> schema_vector;
    for (auto const &field : schema->fields()) {
        auto t = field->type();
        auto new_type = t;
        if (t->Equals(arrow::uint8())) {
            new_type = arrow::int16();
        } else if (t->Equals(arrow::uint16())) {
            new_type = arrow::int32();
        } else if (t->Equals(arrow::uint32()) || t->Equals(arrow::uint64())) {
            new_type = arrow::int64();
        } else if (t->Equals(arrow::list(arrow::uint64()))) {
            continue;
        }
        auto new_field = std::make_shared<arrow::Field>(field->name(), new_type);
        schema_vector.emplace_back(new_field);
    }

    return std::make_shared<arrow::Schema>(schema_vector);
}

void write_table(const arrow::Table *from, parquet::arrow::FileWriter *writer) {  // NOLINT
    auto const &new_schema = writer->schema();
    auto const &old_schema = from->schema();
    auto num_chunks = from->column(0)->num_chunks();
    auto new_field_names = new_schema->field_names();
    auto names = std::unordered_set(new_field_names.begin(), new_field_names.end());
    auto *pool = arrow::default_memory_pool();

    for (int chunk_idx = 0; chunk_idx < num_chunks; chunk_idx++) {
        auto chunk = writer->NewRowGroup(from->column(0)->chunk(chunk_idx)->length());
        if (!chunk.ok()) return;

        for (int i = 0; i < from->num_columns(); i++) {
            auto field = old_schema->field(i);
            auto new_field = new_schema->GetFieldByName(field->name());
            // drop that column
            if (names.find(field->name()) == names.end()) continue;
            auto const &type = new_field->type();
            auto const &column = from->column(i)->chunk(chunk_idx);
            // depends on the type we have different builder
            std::shared_ptr<arrow::Array> array;
            if (type->Equals(arrow::boolean()) || type->Equals(arrow::utf8())) {
                array = column;
            } else if (type->Equals(arrow::int16())) {
                auto builder = arrow::Int16Builder(pool);
                for (int row = 0; row < column->length(); row++) {
                    (void)builder.Append(
                        hermes::get_int16(*(*column->GetScalar(row))->CastTo(arrow::int16())));
                }
                (void)builder.Finish(&array);
            } else if (type->Equals(arrow::int32())) {
                auto builder = arrow::Int32Builder(pool);
                for (int row = 0; row < column->length(); row++) {
                    (void)builder.Append(
                        hermes::get_int32(*(*column->GetScalar(row))->CastTo(arrow::int32())));
                }
                (void)builder.Finish(&array);
            } else if (type->Equals(arrow::int64())) {
                auto builder = arrow::Int64Builder(pool);
                for (int row = 0; row < column->length(); row++) {
                    (void)builder.Append(
                        hermes::get_int64(*(*column->GetScalar(row))->CastTo(arrow::int64())));
                }
                (void)builder.Finish(&array);
            } else {
                throw std::runtime_error("Unable to convert types");
            }

            // need to cast out each value
            auto r = writer->WriteColumnChunk(*array);
            if (!r.ok()) return;
        }
    }
}

int main(int argc, char *argv[]) {  // NOLINT
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " log-dir output-dir" << std::endl;
        return EXIT_FAILURE;
    }

    hermes::Loader loader(argv[1]);
    auto const *output_dir = argv[2];

    auto fs = hermes::load_fs(hermes::FileSystemInfo(output_dir));
    if (!fs) {
        std::cerr << "Unable to open filesystem " << output_dir << std::endl;
        return EXIT_FAILURE;
    }

    auto res = fs->GetFileInfo(output_dir);
    if (!res.ok()) {
        std::cerr << "Unable to open filesystem " << output_dir << std::endl;
        return EXIT_FAILURE;
    }
    if (!(*res).IsDirectory()) {
        (void)fs->CreateDir(output_dir, true);
    }

    // pretty much default so that spark won't choke
    auto builder = parquet::WriterProperties::Builder();
    auto writer_properties = builder.build();

    // we just open up the files and try to convert the schema and
    auto const &tables = loader.tables();
    std::unordered_map<std::string, std::unique_ptr<parquet::arrow::FileWriter>> writers_;
    for (auto const &[info, table] : tables) {
        auto const *file = info.first;
        auto name = file->filename;
        auto base_name = fs::path(name).filename();
        auto output_filename = fs::path(output_dir) / base_name;

        if (writers_.find(output_filename) == writers_.end()) {
            auto res_f = fs->OpenOutputStream(output_filename);
            if (!res_f.ok()) {
                std::cerr << "Unable to write to " << output_filename << std::endl;
                return EXIT_FAILURE;
            }
            auto out_file = *res_f;

            // need to rewrite schema
            auto old_schema = table->schema();
            auto schema = rewrite_schema(old_schema);

            std::unique_ptr<parquet::arrow::FileWriter> writer;
            auto res = parquet::arrow::FileWriter::Open(
                *schema, arrow::default_memory_pool(), std::move(out_file), writer_properties,
                parquet::default_arrow_writer_properties(), &writer);
            if (!res.ok()) {
                std::cerr << "Unable to write to " << output_filename << std::endl;
                return EXIT_FAILURE;
            }
            writers_.emplace(output_filename, std::move(writer));
        }
        auto &writer = writers_.at(output_filename);
        // write out the table
        write_table(table.get(), writer.get());
    }
    for (auto const &iter : writers_) {
        iter.second->Close();
    }
}
