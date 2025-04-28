#include "sql_highlighter.h"

#include <contrib/libs/re2/re2/re2.h>

#include <util/generic/deque.h>
#include <util/generic/maybe.h>

namespace NSQLHighlight {

    using NSQLTranslationV1::Compile;
    using NSQLTranslationV1::IGenericLexer;
    using NSQLTranslationV1::TGenericToken;

    THashMap<EUnitKind, TString> NamesByUnitKind = [] {
        THashMap<EUnitKind, TString> names;
        names[EUnitKind::Keyword] = "K";
        names[EUnitKind::Punctuation] = "P";
        names[EUnitKind::QuotedIdentifier] = "Q";
        names[EUnitKind::BindParamterIdentifier] = "B";
        names[EUnitKind::TypeIdentifier] = "T";
        names[EUnitKind::FunctionIdentifier] = "F";
        names[EUnitKind::Identifier] = "I";
        names[EUnitKind::Literal] = "L";
        names[EUnitKind::StringLiteral] = "S";
        names[EUnitKind::Comment] = "C";
        names[EUnitKind::Whitespace] = "W";
        names[EUnitKind::Error] = TGenericToken::Error;
        return names;
    }();

    THashMap<TString, EUnitKind> UnitKindsByName = [] {
        THashMap<TString, EUnitKind> kinds;
        for (const auto& [kind, name] : NamesByUnitKind) {
            Y_ENSURE(!kinds.contains(name));
            kinds[name] = kind;
        }
        return kinds;
    }();

    IGenericLexer::TGrammar ToGenericLexerGrammar(THighlighting highlighting) {
        IGenericLexer::TGrammar grammar;
        for (const auto& unit : highlighting.Units) {
            for (const auto& pattern : unit.Patterns) {
                grammar.emplace_back(IGenericLexer::TTokenMatcher{
                    .TokenName = NamesByUnitKind.at(unit.Kind),
                    .Match = Compile(pattern),
                });
            }
        }
        return grammar;
    }

    class THighlighter: public IHighlighter {
    public:
        explicit THighlighter(THighlighting highlighting)
            : Lexer_(NSQLTranslationV1::MakeGenericLexer(
                  ToGenericLexerGrammar(std::move(highlighting))))
        {
        }

        void Tokenize(TStringBuf text, const TTokenCallback& onNext) const override {
            Lexer_->Tokenize(text, [&](NSQLTranslationV1::TGenericToken&& token) {
                if (token.Name == "EOF") {
                    return;
                }

                onNext({
                    .Kind = UnitKindsByName.at(token.Name),
                    .Begin = token.Begin,
                    .Length = token.Content.size(),
                });
            });
        }

    private:
        NSQLTranslationV1::IGenericLexer::TPtr Lexer_;
    };

    TVector<TToken> Tokenize(IHighlighter& highlighter, TStringBuf text) {
        TVector<TToken> tokens;
        highlighter.Tokenize(text, [&](TToken&& token) {
            tokens.emplace_back(std::move(token));
        });
        return tokens;
    }

    IHighlighter::TPtr MakeHighlighter(THighlighting highlighting) {
        return IHighlighter::TPtr(new THighlighter(std::move(highlighting)));
    }

} // namespace NSQLHighlight
