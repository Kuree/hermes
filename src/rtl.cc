#include "rtl.hh"

#include "fmt/format.h"
#include "slang/compilation/Compilation.h"
#include "slang/diagnostics/DiagnosticEngine.h"
#include "slang/parsing/Parser.h"
#include "slang/parsing/Preprocessor.h"
#include "slang/symbols/CompilationUnitSymbols.h"
#include "slang/symbols/VariableSymbols.h"
#include "slang/syntax/SyntaxTree.h"
#include "slang/text/SourceManager.h"
#include "slang/types/AllTypes.h"
#include "slang/types/Type.h"

namespace hermes {

void parse_enum(const slang::Symbol &symbol, EnumMap &enums, std::string &error_message) {
    if (slang::VariableSymbol::isKind(symbol.kind)) {
        // this might be an enum
        auto const &variable = symbol.as<slang::VariableSymbol>();
        auto const &type = variable.getType();
        if (type.isEnum()) {
            auto const &enum_type = type.as<slang::EnumType>();
            auto const &enum_values = enum_type.values();
            for (auto const &enum_value : enum_values) {
                auto const &name = enum_value.name;
                auto const &const_value = enum_value.getValue().integer();
                auto value = const_value.as<uint64_t>();
                if (!value) {
                    error_message = fmt::format("Unable to parse enum {0}", variable.name);
                    return;
                }
                enums.emplace(std::string(name),
                              std::make_pair(std::string(variable.name), *value));
            }
        }
    }
}

void parse_enum(const slang::RootSymbol &root, EnumMap &enums,
                std::unordered_map<std::string, EnumMap> &package_enums,
                std::string &error_message) {
    auto const &compilation_units = root.compilationUnits;
    for (auto const &cu : compilation_units) {
        for (auto const &member : cu->members()) {
            if (slang::VariableSymbol::isKind(member.kind)) {
                parse_enum(member, enums, error_message);
            } else if (slang::PackageSymbol::isKind(member.kind)) {
                auto const &package = member.as<slang::PackageSymbol>();
                for (auto const &package_mem : package.members()) {
                    if (slang::VariableSymbol::isKind(package_mem.kind)) {
                        auto &enums_ = package_enums[std::string(package.name)];
                        parse_enum(package_mem, enums_, error_message);
                    }
                }
            }
        }
    }
}

RTL::RTL(const std::vector<std::string> &files, const std::vector<std::string> &includes) {
    slang::SourceManager source_manager;
    for (const std::string &dir : includes) {
        try {
            source_manager.addUserDirectory(string_view(dir));
        } catch (const std::exception &) {
            error_message_ = fmt::format("include directory {0} does not exist", dir);
            return;
        }
    }
    slang::PreprocessorOptions preprocessor_options;
    slang::LexerOptions lexer_options;
    slang::ParserOptions parser_options;
    slang::CompilationOptions compilation_options;
    compilation_options.suppressUnused = true;

    slang::Bag options;
    options.set(preprocessor_options);
    options.set(lexer_options);
    options.set(parser_options);
    options.set(compilation_options);

    std::vector<slang::SourceBuffer> buffers;
    for (const std::string &file : files) {
        slang::SourceBuffer buffer = source_manager.readSource(file);
        if (!buffer) {
            error_message_ = fmt::format("file {0} does not exist", file);
            return;
        }

        buffers.push_back(buffer);
    }

    slang::Compilation compilation;
    for (const slang::SourceBuffer &buffer : buffers)
        compilation.addSyntaxTree(slang::SyntaxTree::fromBuffer(buffer, source_manager, options));

    slang::DiagnosticEngine diag_engine(source_manager);
    // issuing all diagnosis
    for (auto const &diag : compilation.getAllDiagnostics()) diag_engine.issue(diag);

    if (diag_engine.getNumErrors() > 0) {
        error_message_ = "Error when parsing RTL files.";
    }

    // loop through every enum definition and collect their values
    // we don't use tree based visitor since we are only interested in enums defined in the
    // compilation unit (global) or inside package. SV doesn't allow nested package so there
    // are at most 2 level (including the enum)
    auto const &root = compilation.getRoot();
    parse_enum(root, enums_, package_enums_, error_message_);

    // index enums
    index_enums();
}

std::optional<uint64_t> PackageProxy::get(const std::string &name) const {
    if (values.find(name) != values.end()) {
        return values.at(name).second;
    } else {
        return std::nullopt;
    }
}

PackageProxy::PackageProxy(const EnumMap &values) : values(values) {
    // build index
    // make cache it?
    for (auto const &[value_name, vs] : values) {
        auto const &[def_name, v] = vs;
        index[def_name][value_name] = v;
    }
}

std::optional<uint64_t> RTL::get(const std::string &name) const {
    if (enums_.find(name) != enums_.end()) {
        return enums_.at(name).second;
    } else {
        return std::nullopt;
    }
}

std::shared_ptr<PackageProxy> RTL::package(const std::string &name) const {
    if (packages_.find(name) != packages_.end()) {
        return packages_.at(name);
    } else {
        return nullptr;
    }
}

std::optional<std::string> RTL::lookup(uint64_t value) {
    for (auto const &[enum_name, value_pair] : enums_) {
        auto const &[def_name, v] = value_pair;
        if (value == v) {
            return enum_name;
        }
    }
    return std::nullopt;
}

std::optional<std::string> RTL::lookup(uint64_t value, const std::string &enum_name) {
    return lookup(value, enum_name, enums_);
}

std::optional<std::string> RTL::lookup(uint64_t value, const std::string &pkg_name,
                                       const std::string &enum_name) {
    if (package_enums_.find(pkg_name) != package_enums_.end()) {
        return lookup(value, enum_name, package_enums_.at(pkg_name));
    } else {
        return std::nullopt;
    }
}

std::optional<std::string> RTL::lookup(uint64_t value, const std::string &enum_def_name,
                                       const EnumMap &enum_map) {
    for (auto const &[enum_name_, value_pair] : enum_map) {
        auto const &[def_name, v] = value_pair;
        if (value == v && def_name == enum_def_name) {
            return enum_name_;
        }
    }
    return std::nullopt;
}

void RTL::index_enums() {
    packages_.emplace("", std::make_shared<PackageProxy>(enums_));
    for (auto const &[name, map] : package_enums_) {
        packages_.emplace(name, std::make_shared<PackageProxy>(map));
    }
}

}  // namespace hermes