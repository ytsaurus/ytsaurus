#pragma once

#include "c3i.h"

#include <yql/essentials/sql/v1/complete/text/word.h>

#include <contrib/libs/antlr4_cpp_runtime/src/ANTLRInputStream.h>
#include <contrib/libs/antlr4_cpp_runtime/src/BufferedTokenStream.h>
#include <contrib/libs/antlr4_cpp_runtime/src/Vocabulary.h>
#include <contrib/libs/antlr4-c3/src/CodeCompletionCore.hpp>

#include <util/generic/fwd.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

namespace NSQLComplete {

    template <class Lexer, class Parser>
    struct TAntlrGrammar {
        using TLexer = Lexer;
        using TParser = Parser;

        TAntlrGrammar() = delete;
    };

    template <class G>
    class TC3Engine: public IC3Engine {
    public:
        explicit TC3Engine(TConfig config)
            : Chars()
            , Lexer(&Chars)
            , Tokens(&Lexer)
            , Parser(&Tokens)
            , CompletionCore(&Parser)
            , PunctuationTokens(std::move(config.PunctuationTokens))
        {
            Lexer.removeErrorListeners();
            Parser.removeErrorListeners();

            CompletionCore.ignoredTokens = std::move(config.IgnoredTokens);
            CompletionCore.preferredRules = std::move(config.PreferredRules);
        }

        TC3Candidates Complete(TCompletionInput input) override {
            Assign(input.Text.Head(input.CursorPosition));
            const auto caretTokenIndex = CaretTokenIndex();
            auto candidates = CompletionCore.collectCandidates(caretTokenIndex);
            return Converted(std::move(candidates));
        }

    private:
        void Assign(TStringBuf prefix) {
            Chars.load(prefix.Data(), prefix.Size(), /* lenient = */ false);
            Lexer.reset();
            Tokens.setTokenSource(&Lexer);
            Tokens.fill();
        }

        size_t CaretTokenIndex() {
            Y_ENSURE(0 < Tokens.size());
            auto index = Tokens.size() - 1;
            if (1 <= index) {
                antlr4::Token* token = Tokens.get(index - 1);
                if (!PunctuationTokens.contains(token->getType())) {
                    index -= 1;
                }
            }
            return index;
        }

        static TC3Candidates Converted(c3::CandidatesCollection candidates) {
            TC3Candidates converted;
            for (auto& [token, following] : candidates.tokens) {
                converted.Tokens.emplace_back(token, std::move(following));
            }
            for (auto& [rule, data] : candidates.rules) {
                converted.Rules.emplace_back(rule, std::move(data.ruleList));
                converted.Rules.back().ParserCallStack.emplace_back(rule);
            }
            return converted;
        }

        antlr4::ANTLRInputStream Chars;
        G::TLexer Lexer;
        antlr4::BufferedTokenStream Tokens;
        G::TParser Parser;
        c3::CodeCompletionCore CompletionCore;
        std::unordered_set<TTokenId> PunctuationTokens;
    };

} // namespace NSQLComplete
