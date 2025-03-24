#include "sql_context.h"

#include "c3_engine.h"
#include "sql_syntax.h"

#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

#include <util/generic/algorithm.h>
#include <util/stream/output.h>

namespace NSQLComplete {

    template <bool IsAnsiLexer>
    class TSpecializedSqlContextInference: public ISqlContextInference {
    private:
        using TDefaultYQLGrammar = TAntlrGrammar<
            NALADefaultAntlr4::SQLv1Antlr4Lexer,
            NALADefaultAntlr4::SQLv1Antlr4Parser>;

        using TAnsiYQLGrammar = TAntlrGrammar<
            NALAAnsiAntlr4::SQLv1Antlr4Lexer,
            NALAAnsiAntlr4::SQLv1Antlr4Parser>;

        using G = std::conditional_t<
            IsAnsiLexer,
            TAnsiYQLGrammar,
            TDefaultYQLGrammar>;

    public:
        TSpecializedSqlContextInference()
            : Grammar(&GetSqlGrammar(IsAnsiLexer))
            , C3(ComputeC3Config())
        {
            NSQLTranslationV1::TLexers lexers;
            lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
            lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();

            Lexer_ = NSQLTranslationV1::MakeLexer(lexers, IsAnsiLexer, /* antlr4 = */ true, /* pure = */ true);
        }

        TCompletionContext Analyze(TCompletionInput input) override {
            auto tokens = C3.Complete(C3Prefix(input));
            return {
                .Keywords = SiftedKeywords(tokens),
            };
        }

    private:
        IC3Engine::TConfig ComputeC3Config() {
            return {
                .IgnoredTokens = ComputeIgnoredTokens(),
                .PreferredRules = ComputePreferredRules(),
            };
        }

        std::unordered_set<TTokenId> ComputeIgnoredTokens() {
            auto ignoredTokens = Grammar->GetAllTokens();
            for (auto keywordToken : Grammar->GetKeywordTokens()) {
                ignoredTokens.erase(keywordToken);
            }
            return ignoredTokens;
        }

        std::unordered_set<TRuleId> ComputePreferredRules() {
            const auto& keywordRules = Grammar->GetKeywordRules();

            std::unordered_set<TRuleId> preferredRules;

            // Excludes tokens obtained from keyword rules
            preferredRules.insert(std::begin(keywordRules), std::end(keywordRules));

            return preferredRules;
        }

        const TStringBuf C3Prefix(TCompletionInput input) {
            const TStringBuf prefix = input.Text.Head(input.CursorPosition);

            TVector<TString> statements;
            NYql::TIssues issues;
            if (!NSQLTranslationV1::SplitQueryToStatements(
                    TString(prefix) + (prefix.EndsWith(';') ? ";" : ""), Lexer_,
                    statements, issues, /* file = */ "",
                    /* areBlankSkipped = */ false)) {
                return prefix;
            }

            if (statements.empty()) {
                return prefix;
            }

            return prefix.Last(statements.back().size());
        }

        TVector<TString> SiftedKeywords(const TVector<TSuggestedToken>& tokens) {
            const auto& vocabulary = Grammar->GetVocabulary();
            const auto& keywordTokens = Grammar->GetKeywordTokens();

            TVector<TString> keywords;
            for (const auto& token : tokens) {
                if (keywordTokens.contains(token.Number)) {
                    keywords.emplace_back(vocabulary.getDisplayName(token.Number));
                }
            }
            return keywords;
        }

        const ISqlGrammar* Grammar;
        NSQLTranslation::ILexer::TPtr Lexer_;
        TC3Engine<G> C3;
    };

    class TSqlContextInference: public ISqlContextInference {
    public:
        TCompletionContext Analyze(TCompletionInput input) override {
            auto isAnsiLexer = IsAnsiQuery(TString(input.Text));
            auto& engine = GetSpecializedEngine(isAnsiLexer);
            return engine.Analyze(std::move(input));
        }

    private:
        ISqlContextInference& GetSpecializedEngine(bool isAnsiLexer) {
            if (isAnsiLexer) {
                return AnsiEngine;
            }
            return DefaultEngine;
        }

        TSpecializedSqlContextInference</* IsAnsiLexer = */ false> DefaultEngine;
        TSpecializedSqlContextInference</* IsAnsiLexer = */ true> AnsiEngine;
    };

    ISqlContextInference::TPtr MakeSqlContextInference() {
        return TSqlContextInference::TPtr(new TSqlContextInference());
    }

} // namespace NSQLComplete
