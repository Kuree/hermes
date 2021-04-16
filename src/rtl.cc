#include "rtl.hh"

#include "fmt/format.h"
#include "slang/compilation/Compilation.h"
#include "slang/parsing/Parser.h"
#include "slang/parsing/Preprocessor.h"
#include "slang/syntax/SyntaxTree.h"
#include "slang/text/SourceManager.h"

namespace hermes {
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

    // loop through every enum definition and collect their values
}
}  // namespace hermes