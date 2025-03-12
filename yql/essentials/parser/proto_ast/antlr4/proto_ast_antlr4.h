#pragma once

#include <yql/essentials/parser/proto_ast/common.h>

#include <yql/essentials/parser/antlr_ast/antlr4/antlr_ast_antlr4.h>

#ifdef ERROR
#undef ERROR
#endif
#include <contrib/libs/antlr4_cpp_runtime/src/antlr4-runtime.h>

namespace NProtoAST {
    using namespace NAST;

    template <typename TParser, typename TLexer>
    class TProtoASTBuilder4 {

    public:
        TProtoASTBuilder4(TStringBuf data, const TString& queryName = "query", google::protobuf::Arena* arena = nullptr)
            : QueryName(queryName)
            , InputStream(data)
            , Lexer(&InputStream)
            , TokenStream(&Lexer)
            , Parser(&TokenStream, arena)
        {
        }

        google::protobuf::Message* BuildAST(IErrorCollector& errors) {
            // TODO: find a better way to break on lexer errors
            typename antlr4::YqlErrorListener listener(&errors, &Parser.error);
            Parser.removeErrorListeners();
            Parser.addErrorListener(&listener);
            try {
                auto result = Parser.Parse(&errors);
                Parser.removeErrorListener(&listener);
                Parser.error = false;
                return result;
            } catch (const TTooManyErrors&) {
                Parser.removeErrorListener(&listener);
                Parser.error = false;
                return nullptr;
            } catch (...) {
                errors.Error(0, 0, CurrentExceptionMessage());
                Parser.removeErrorListener(&listener);
                Parser.error = false;
                return nullptr;
            }
        }

    private:
        TString QueryName;

        antlr4::ANTLRInputStream InputStream;
        TLexer Lexer;

        antlr4::CommonTokenStream TokenStream;
        TParser Parser;
    };

} // namespace NProtoAST

