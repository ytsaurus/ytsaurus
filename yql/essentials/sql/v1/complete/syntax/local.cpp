#include "local.h"

#include "ansi.h"
#include "grammar.h"
#include "parser_call_stack.h"
#include "token.h"

#include <yql/essentials/sql/v1/complete/antlr4/c3i.h>
#include <yql/essentials/sql/v1/complete/antlr4/c3t.h>
#include <yql/essentials/sql/v1/complete/antlr4/vocabulary.h>

#include <yql/essentials/core/issue/yql_issue.h>

#include <util/generic/algorithm.h>
#include <util/stream/output.h>

#ifdef TOKEN_QUERY // Conflict with the winnt.h
    #undef TOKEN_QUERY
#endif
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

namespace NSQLComplete {

    template <std::regular_invocable<TParserCallStack> StackPredicate>
    std::regular_invocable<TMatchedRule> auto RuleAdapted(StackPredicate predicate) {
        return [=](const TMatchedRule& rule) {
            return predicate(rule.ParserCallStack);
        };
    }

    template <bool IsAnsiLexer>
    class TSpecializedLocalSyntaxAnalysis: public ILocalSyntaxAnalysis {
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
        explicit TSpecializedLocalSyntaxAnalysis(TLexerSupplier lexer)
            : Grammar(&GetSqlGrammar())
            , Lexer_(lexer(/* ansi = */ IsAnsiLexer))
            , C3(ComputeC3Config())
        {
        }

        TLocalSyntaxContext Analyze(TCompletionInput input) override {
            TCompletionInput statement;
            size_t statement_position;
            if (!GetStatement(Lexer_, input, statement, statement_position)) {
                return {};
            }

            TCursorTokenContext context;
            if (!GetCursorTokenContext(Lexer_, statement, context)) {
                return {};
            }

            TC3Candidates candidates = C3.Complete(statement);

            TLocalSyntaxContext result;

            result.EditRange = EditRange(context);
            result.EditRange.Begin += statement_position;

            if (auto enclosing = context.Enclosing()) {
                if (enclosing->IsLiteral()) {
                    return result;
                } else if (enclosing->Base->Name == "ID_QUOTED") {
                    result.Object = ObjectMatch(context, candidates);
                    return result;
                }
            }

            result.Keywords = SiftedKeywords(candidates);
            result.Pragma = PragmaMatch(context, candidates);
            result.IsTypeName = IsTypeNameMatched(candidates);
            result.Function = FunctionMatch(context, candidates);
            result.Hint = HintMatch(candidates);
            result.Object = ObjectMatch(context, candidates);

            return result;
        }

    private:
        IC3Engine::TConfig ComputeC3Config() const {
            return {
                .IgnoredTokens = ComputeIgnoredTokens(),
                .PreferredRules = ComputePreferredRules(),
            };
        }

        std::unordered_set<TTokenId> ComputeIgnoredTokens() const {
            auto ignoredTokens = Grammar->GetAllTokens();
            for (auto keywordToken : Grammar->GetKeywordTokens()) {
                ignoredTokens.erase(keywordToken);
            }
            for (auto punctuationToken : Grammar->GetPunctuationTokens()) {
                ignoredTokens.erase(punctuationToken);
            }
            return ignoredTokens;
        }

        std::unordered_set<TRuleId> ComputePreferredRules() const {
            return GetC3PreferredRules();
        }

        TLocalSyntaxContext::TKeywords SiftedKeywords(const TC3Candidates& candidates) const {
            const auto& vocabulary = Grammar->GetVocabulary();
            const auto& keywordTokens = Grammar->GetKeywordTokens();

            TLocalSyntaxContext::TKeywords keywords;
            for (const auto& token : candidates.Tokens) {
                if (keywordTokens.contains(token.Number)) {
                    auto& following = keywords[Display(vocabulary, token.Number)];
                    for (auto next : token.Following) {
                        following.emplace_back(Display(vocabulary, next));
                    }
                }
            }
            return keywords;
        }

        TMaybe<TLocalSyntaxContext::TPragma> PragmaMatch(
            const TCursorTokenContext& context, const TC3Candidates& candidates) const {
            if (!AnyOf(candidates.Rules, RuleAdapted(IsLikelyPragmaStack))) {
                return Nothing();
            }

            TLocalSyntaxContext::TPragma pragma;
            TMaybe<TRichParsedToken> begin;
            if ((begin = context.MatchCursorPrefix({"ID_PLAIN", "DOT"})) ||
                (begin = context.MatchCursorPrefix({"ID_PLAIN", "DOT", ""}))) {
                pragma.Namespace = begin->Base->Content;
            }
            return pragma;
        }

        bool IsTypeNameMatched(const TC3Candidates& candidates) const {
            return AnyOf(candidates.Rules, RuleAdapted(IsLikelyTypeStack));
        }

        TMaybe<TLocalSyntaxContext::TFunction> FunctionMatch(
            const TCursorTokenContext& context, const TC3Candidates& candidates) const {
            if (!AnyOf(candidates.Rules, RuleAdapted(IsLikelyFunctionStack))) {
                return Nothing();
            }

            TLocalSyntaxContext::TFunction function;
            TMaybe<TRichParsedToken> begin;
            if ((begin = context.MatchCursorPrefix({"ID_PLAIN", "NAMESPACE"})) ||
                (begin = context.MatchCursorPrefix({"ID_PLAIN", "NAMESPACE", ""}))) {
                function.Namespace = begin->Base->Content;
            }
            return function;
        }

        TMaybe<TLocalSyntaxContext::THint> HintMatch(const TC3Candidates& candidates) const {
            // TODO(YQL-19747): detect local contexts with a single iteration through the candidates.Rules
            auto rule = FindIf(candidates.Rules, RuleAdapted(IsLikelyHintStack));
            if (rule == std::end(candidates.Rules)) {
                return Nothing();
            }

            auto stmt = StatementKindOf(rule->ParserCallStack);
            if (stmt.Empty()) {
                return Nothing();
            }

            return TLocalSyntaxContext::THint{
                .StatementKind = *stmt,
            };
        }

        TMaybe<TLocalSyntaxContext::TObject> ObjectMatch(
            const TCursorTokenContext& context, const TC3Candidates& candidates) const {
            TLocalSyntaxContext::TObject object;

            if (AnyOf(candidates.Rules, RuleAdapted(IsLikelyExistingTableStack))) {
                object.Kinds.emplace(TLocalSyntaxContext::TObject::EKind::Folder);
                object.Kinds.emplace(TLocalSyntaxContext::TObject::EKind::Table);
            }

            if (object.Kinds.empty()) {
                return Nothing();
            }

            if (auto path = ObjectPath(context)) {
                object.Path = *path;
                object.IsEnclosed = true;
            }

            return object;
        }

        TMaybe<TString> ObjectPath(const TCursorTokenContext& context) const {
            if (auto enclosing = context.Enclosing();
                enclosing.Defined() && enclosing->Base->Name == "ID_QUOTED") {
                TString path = enclosing->Base->Content;
                path.erase(0, 1);
                path.pop_back();
                return path;
            }
            return "";
        }

        TEditRange EditRange(const TCursorTokenContext& context) const {
            if (auto enclosing = context.Enclosing()) {
                return EditRange(*enclosing, context.Cursor);
            }

            const TRichParsedToken prev = context.TokenAt(context.Cursor.PrevTokenIndex);
            if (IsWordBoundary(prev.Base->Content.back())) {
                return {
                    .Begin = context.Cursor.Position,
                };
            }

            return EditRange(prev, context.Cursor);
        }

        TEditRange EditRange(const TRichParsedToken& token, const TCursor& cursor) const {
            size_t begin = token.Position;
            if (token.Base->Name == "NOT_EQUALS2") {
                begin += 1;
            }
            return {
                .Begin = begin,
                .Length = cursor.Position - begin,
            };
        }

        const ISqlGrammar* Grammar;
        NSQLTranslation::ILexer::TPtr Lexer_;
        TC3Engine<G> C3;
    };

    class TLocalSyntaxAnalysis: public ILocalSyntaxAnalysis {
    public:
        explicit TLocalSyntaxAnalysis(TLexerSupplier lexer)
            : DefaultEngine(lexer)
            , AnsiEngine(lexer)
        {
        }

        TLocalSyntaxContext Analyze(TCompletionInput input) override {
            auto isAnsiLexer = IsAnsiQuery(TString(input.Text));
            auto& engine = GetSpecializedEngine(isAnsiLexer);
            return engine.Analyze(std::move(input));
        }

    private:
        ILocalSyntaxAnalysis& GetSpecializedEngine(bool isAnsiLexer) {
            if (isAnsiLexer) {
                return AnsiEngine;
            }
            return DefaultEngine;
        }

        TSpecializedLocalSyntaxAnalysis</* IsAnsiLexer = */ false> DefaultEngine;
        TSpecializedLocalSyntaxAnalysis</* IsAnsiLexer = */ true> AnsiEngine;
    };

    ILocalSyntaxAnalysis::TPtr MakeLocalSyntaxAnalysis(TLexerSupplier lexer) {
        return MakeHolder<TLocalSyntaxAnalysis>(lexer);
    }

} // namespace NSQLComplete
