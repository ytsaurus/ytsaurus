#pragma once

#include <yql/essentials/parser/common/error.h>
#include <yql/essentials/parser/lexer_common/lexer.h>

#include <contrib/libs/antlr4_cpp_runtime/src/antlr4-runtime.h>

namespace antlr4 {

    class ANTLR4CPP_PUBLIC YqlErrorListener: public BaseErrorListener {
        NAST::IErrorCollector* errors;
        bool* error;

    public:
        YqlErrorListener(NAST::IErrorCollector* errors, bool* error);

        virtual void syntaxError(
            Recognizer* recognizer, Token* offendingSymbol,
            size_t line, size_t charPositionInLine,
            const std::string& msg, std::exception_ptr e) override;
    };

} // namespace antlr4

namespace NAST {

    template <>
    inline void InvalidToken<antlr4::Token>(IOutputStream& err, const antlr4::Token* token) {
        if (token) {
            if (token->getInputStream()) {
                err << " '" << token->getText() << "'";
            } else {
                err << ABSENCE;
            }
        }
    }

} // namespace NAST

namespace NAntlrAST {

    template <typename TLexer>
    class TLexerTokensCollector4 {
    public:
        TLexerTokensCollector4(TStringBuf data, const TString& queryName = "query")
            : QueryName(queryName)
            , InputStream(std::string(data))
            , Lexer(&InputStream)
        {
        }

        void CollectTokens(NAST::IErrorCollector& errors, const NSQLTranslation::ILexer::TTokenCallback& onNextToken) {
            try {
                bool error = false;
                typename antlr4::YqlErrorListener listener(&errors, &error);
                Lexer.removeErrorListeners();
                Lexer.addErrorListener(&listener);

                for (;;) {
                    auto token = Lexer.nextToken();
                    auto type = token->getType();
                    const bool isEOF = type == TLexer::EOF;
                    NSQLTranslation::TParsedToken last;
                    last.Name = GetTokenName(type);
                    last.Content = token->getText();
                    last.Line = token->getLine();
                    last.LinePos = token->getCharPositionInLine();
                    onNextToken(std::move(last));
                    if (isEOF) {
                        break;
                    }
                }
            } catch (const NAST::TTooManyErrors&) {
            } catch (...) {
                errors.Error(0, 0, CurrentExceptionMessage());
            }
        }

    private:
        TString GetTokenName(size_t type) const {
            auto res = Lexer.getVocabulary().getSymbolicName(type);
            if (res != "") {
                return TString(res);
            }
            return TString(NAST::INVALID_TOKEN_NAME);
        }

        TString QueryName;
        antlr4::ANTLRInputStream InputStream;
        TLexer Lexer;
    };

} // namespace NAntlrAST
