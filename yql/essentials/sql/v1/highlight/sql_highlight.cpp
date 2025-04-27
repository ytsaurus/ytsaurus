#include "sql_highlight.h"

#include <yql/essentials/sql/v1/lexer/regex/regex.h>

#include <contrib/libs/re2/re2/re2.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NSQLHighlight {

    bool TPattern::IsPlain() const {
        static RE2 PlainRe("[a-zA-Z]+");
        return RE2::FullMatch(BodyRe, PlainRe) &&
               RE2::FullMatch(AfterRe, PlainRe);
    }

    struct Syntax {
        NSQLReflect::TLexerGrammar Grammar;
        THashMap<TString, TString> RegexByOtherName;

        TString Concat(const TVector<TStringBuf>& names) {
            TString concat;
            for (const auto& name : names) {
                concat += Get(name);
            }
            return concat;
        }

        TString Get(const TStringBuf name) const {
            if (Grammar.PunctuationNames.contains(name)) {
                return RE2::QuoteMeta(Grammar.BlockByName.at(name));
            }
            return RegexByOtherName.at(name);
        }
    };

    template <EUnitKind K>
    TUnit MakeUnit(Syntax& syntax);

    template <>
    TUnit MakeUnit<EUnitKind::Keyword>(Syntax& s) {
        using NSQLReflect::TLexerGrammar;

        TUnit unit = {.Kind = EUnitKind::Keyword};
        for (const auto& keyword : s.Grammar.KeywordNames) {
            const TStringBuf content = TLexerGrammar::KeywordBlock(keyword);
            unit.Patterns.push_back({TString(content)});
        }
        return unit;
    }

    template <>
    TUnit MakeUnit<EUnitKind::Punctuation>(Syntax& s) {
        TUnit unit = {.Kind = EUnitKind::Punctuation};
        for (const auto& name : s.Grammar.PunctuationNames) {
            const TString content = s.Get(name);
            unit.Patterns.push_back({content});
        }
        return unit;
    }

    template <>
    TUnit MakeUnit<EUnitKind::Identifier>(Syntax& s) {
        return {
            .Kind = EUnitKind::Identifier,
            .Patterns = {
                {s.Get("ID_PLAIN")},
            },
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::QuotedIdentifier>(Syntax& s) {
        return {
            .Kind = EUnitKind::QuotedIdentifier,
            .Patterns = {
                {s.Get("ID_QUOTED")},
            },
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::BindParamterIdentifier>(Syntax& s) {
        return {
            .Kind = EUnitKind::BindParamterIdentifier,
            .Patterns = {
                {s.Concat({"DOLLAR", "ID_PLAIN"})},
            },
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::TypeIdentifier>(Syntax& s) {
        return {
            .Kind = EUnitKind::TypeIdentifier,
            .Patterns = {
                {s.Get("ID_PLAIN"), s.Get("LESS")},
                {"Decimal"},
                {"Bool"},
                {"Int8"},
                {"Int16"},
                {"Int32"},
                {"Int64"},
                {"Uint8"},
                {"Uint16"},
                {"Uint32"},
                {"Uint64"},
                {"Float"},
                {"Double"},
                {"DyNumber"},
                {"String"},
                {"Utf8"},
                {"Json"},
                {"JsonDocument"},
                {"Yson"},
                {"Uuid"},
                {"Date"},
                {"Datetime"},
                {"Timestamp"},
                {"Interval"},
                {"TzDate"},
                {"TzDateTime"},
                {"TzTimestamp"},
                {"Callable"},
                {"Resource"},
                {"Tagged"},
                {"Generic"},
                {"Unit"},
                {"Null"},
                {"Void"},
                {"EmptyList"},
                {"EmptyDict"},
            },
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::FunctionIdentifier>(Syntax& s) {
        return {
            .Kind = EUnitKind::FunctionIdentifier,
            .Patterns = {
                {s.Concat({"ID_PLAIN", "NAMESPACE", "ID_PLAIN"})},
                {s.Get("ID_PLAIN"), s.Get("LPAREN")},
            },
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::Literal>(Syntax& s) {
        return {
            .Kind = EUnitKind::Literal,
            .Patterns = {
                {s.Get("DIGITS")},
                {s.Get("INTEGER_VALUE")},
                {s.Get("REAL")},
            },
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::StringLiteral>(Syntax& s) {
        return {
            .Kind = EUnitKind::StringLiteral,
            .Patterns = {
                {s.Get("STRING_VALUE")},
            },
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::Comment>(Syntax& s) {
        return {
            .Kind = EUnitKind::Comment,
            .Patterns = {
                {s.Get("COMMENT")},
            },
        };
    }

    Syntax MakeSyntax(NSQLReflect::TLexerGrammar grammar) {
        Syntax syntax;
        syntax.Grammar = std::move(grammar);
        for (auto& [k, v] : NSQLTranslationV1::MakeRegexByOtherName(syntax.Grammar, /* ansi = */ false)) {
            syntax.RegexByOtherName.emplace(std::move(k), std::move(v));
        }
        return syntax;
    }

    THighlighting MakeHighlighting(NSQLReflect::TLexerGrammar grammar) {
        Syntax syntax = MakeSyntax(std::move(grammar));

        THighlighting h;
        h.Units.emplace_back(MakeUnit<EUnitKind::Keyword>(syntax));
        h.Units.emplace_back(MakeUnit<EUnitKind::Punctuation>(syntax));
        h.Units.emplace_back(MakeUnit<EUnitKind::Identifier>(syntax));
        h.Units.emplace_back(MakeUnit<EUnitKind::QuotedIdentifier>(syntax));
        h.Units.emplace_back(MakeUnit<EUnitKind::BindParamterIdentifier>(syntax));
        h.Units.emplace_back(MakeUnit<EUnitKind::TypeIdentifier>(syntax));
        h.Units.emplace_back(MakeUnit<EUnitKind::FunctionIdentifier>(syntax));
        h.Units.emplace_back(MakeUnit<EUnitKind::Literal>(syntax));
        h.Units.emplace_back(MakeUnit<EUnitKind::StringLiteral>(syntax));
        h.Units.emplace_back(MakeUnit<EUnitKind::Comment>(syntax));

        h.Whitespace = {syntax.Get("WS")};

        return h;
    }

} // namespace NSQLHighlight

template <>
void Out<NSQLHighlight::EUnitKind>(IOutputStream& out, NSQLHighlight::EUnitKind kind) {
    switch (kind) {
        case NSQLHighlight::EUnitKind::Keyword:
            out << "Keyword";
            break;
        case NSQLHighlight::EUnitKind::Punctuation:
            out << "Punctuation";
            break;
        case NSQLHighlight::EUnitKind::Identifier:
            out << "Identifier";
            break;
        case NSQLHighlight::EUnitKind::QuotedIdentifier:
            out << "QuotedIdentifier";
            break;
        case NSQLHighlight::EUnitKind::BindParamterIdentifier:
            out << "BindParamterIdentifier";
            break;
        case NSQLHighlight::EUnitKind::TypeIdentifier:
            out << "TypeIdentifier";
            break;
        case NSQLHighlight::EUnitKind::FunctionIdentifier:
            out << "FunctionIdentifier";
            break;
        case NSQLHighlight::EUnitKind::Literal:
            out << "Literal";
            break;
        case NSQLHighlight::EUnitKind::StringLiteral:
            out << "StringLiteral";
            break;
        case NSQLHighlight::EUnitKind::Comment:
            out << "Comment";
            break;
    }
}
