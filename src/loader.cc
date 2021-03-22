#include "loader.hh"

#include <filesystem>
#include <fstream>

#include "arrow/api.h"
#include "fmt/format.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace fs = std::filesystem;

namespace hermes {

Loader::Loader(std::string dir) : dir_(fs::absolute(std::move(dir))) {
    // we can't use arrow's filesystem to handle this because the default public API
    // doesn't have proper interface exposed. in the future we will directly use the
    // internal API
    // do a LIST operation
    for (auto const &entry : fs::directory_iterator(dir_)) {
        auto path = fs::path(entry);
        if (!fs::is_regular_file(path)) {
            // we ara only interested in regular file
            continue;
        }
        auto ext = path.extension();
        if (ext == ".json") {
            // json file, parse it and load it up
            load_json(path);
        }
    }
}

static bool check_member(rapidjson::Document &document, const char *member_name) {
    return document.HasMember(member_name);
}

template <typename T>
static std::optional<T> get_member(rapidjson::Document &document, const char *member_name) {
    if (!check_member(document, member_name)) return std::nullopt;
    if constexpr (std::is_same<T, std::string>::value) {
        if (document[member_name].IsString()) {
            return std::string(document[member_name].GetString());
        }
    } else if constexpr (std::is_integral<T>::value && !std::is_same<T, bool>::value) {
        if (document[member_name].IsNumber()) {
            return document[member_name].template Get<T>();
        }
    } else if constexpr (std::is_same<T, bool>::value) {
        if (document[member_name].IsBool()) {
            return document[member_name].GetBool();
        }
    }
    return std::nullopt;
}

void Loader::load_json(const std::string &path) {
    std::string content;
    std::ifstream stream(path);
    if (stream.bad()) return;
    stream >> content;

    rapidjson::Document document;
    document.Parse(content.c_str());
    if (document.HasParseError()) return;
    // get indexed value
    auto opt_parquet_file = get_member<std::string>(document, "parquet");
    if (!opt_parquet_file) return;
    auto const &parquet_file = *opt_parquet_file;

    auto opt_type = get_member<std::string>(document, "type");
    if (!opt_type) return;
    auto const &type = *opt_type;

    std::pair<uint64_t, uint64_t> ids, times;
    const static std::vector minmax = {"max", "min"};
    for (auto i = 0; i < 2; i++) {
        auto name = fmt::format("{0}_id", minmax[i]);
        auto value = get_member<uint64_t>(document, name.c_str());
        if (!value) return;
        if (i == 0)
            ids.first = *value;
        else
            ids.second = *value;
        name = fmt::format("{0}_time", minmax[i]);
        value = get_member<uint64_t>(document, name.c_str());
        if (!value) return;
        if (i == 0)
            times.first = *value;
        else
            times.second = *value;
    }

    auto info = std::make_unique<FileInfo>(path, ids.first, ids.second, times.first, times.second);
    if (type == "event") {
        events_.emplace_back(info.get());
    } else if (type == "transaction") {
        transactions_.emplace_back(info.get());
    } else {
        return;
    }
    files_.emplace_back(std::move(info));
}

}  // namespace hermes