#include "sql_highlight.h"

#include <yql/essentials/sql/v1/lexer/regex/regex.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace SQLHighlight {

    struct Syntax {
        NSQLReflect::TLexerGrammar Grammar;
        THashMap<TString, TString> RegexByOtherName;
        THashSet<TString> Referenced;

        TString Ref(const TStringBuf name) {
            Y_ENSURE(
                Grammar.PunctuationNames.contains(name) ||
                    RegexByOtherName.contains(name), name);
            Referenced.emplace(name);
            return TString(name);
        }

        TString Inl(const TStringBuf name) const {
            if (Grammar.PunctuationNames.contains(name)) {
                return Grammar.BlockByName.at(name);
            }
            return RegexByOtherName.at(name);
        }
    };

    template <EHUnitKind K>
    THumanHighlighting::TUnit MakeUnit(Syntax& syntax);

    template <>
    THumanHighlighting::TUnit MakeUnit<EHUnitKind::Keyword>(Syntax& syntax) {
        THumanHighlighting::TUnit unit = {.Kind = EHUnitKind::Keyword};
        for (const auto& keyword : syntax.Grammar.KeywordNames) {
            const TStringBuf content = NSQLReflect::TLexerGrammar::KeywordBlock(keyword);
            unit.Tokens.push_back({TString(content)});
        }
        return unit;
    }

    template <>
    THumanHighlighting::TUnit MakeUnit<EHUnitKind::Punctuation>(Syntax& syntax) {
        THumanHighlighting::TUnit unit = {.Kind = EHUnitKind::Punctuation};
        for (const auto& name : syntax.Grammar.PunctuationNames) {
            const TString content = syntax.Grammar.BlockByName.at(name);
            unit.Tokens.push_back({content});
        }
        return unit;
    }

    template <>
    THumanHighlighting::TUnit MakeUnit<EHUnitKind::Identifier>(Syntax& syntax) {
        return {
            .Kind = EHUnitKind::Identifier,
            .Tokens = {
                {syntax.Ref("ID_PLAIN")},
            },
        };
    }

    template <>
    THumanHighlighting::TUnit MakeUnit<EHUnitKind::QuotedIdentifier>(Syntax& syntax) {
        return {
            .Kind = EHUnitKind::QuotedIdentifier,
            .Tokens = {
                {syntax.Inl("ID_QUOTED")},
            },
        };
    }

    template <>
    THumanHighlighting::TUnit MakeUnit<EHUnitKind::BindParamterIdentifier>(Syntax& syntax) {
        return {
            .Kind = EHUnitKind::BindParamterIdentifier,
            .Tokens = {
                {syntax.Inl("DOLLAR"), syntax.Ref("ID_PLAIN")},
            },
        };
    }

    template <>
    THumanHighlighting::TUnit MakeUnit<EHUnitKind::TypeIdentifier>(Syntax& syntax) {
        return {
            .Kind = EHUnitKind::TypeIdentifier,
            .Tokens = {
                {syntax.Ref("ID_PLAIN"), syntax.Inl("LESS")},
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
    THumanHighlighting::TUnit MakeUnit<EHUnitKind::FunctionIdentifier>(Syntax& syntax) {
        return {
            .Kind = EHUnitKind::FunctionIdentifier,
            .Tokens = {
                {syntax.Ref("ID_PLAIN"), syntax.Inl("NAMESPACE"), syntax.Ref("ID_PLAIN")},
                {syntax.Ref("ID_PLAIN"), syntax.Inl("LPAREN")},
            },
        };
    }

    template <>
    THumanHighlighting::TUnit MakeUnit<EHUnitKind::Literal>(Syntax& syntax) {
        return {
            .Kind = EHUnitKind::Literal,
            .Tokens = {
                {syntax.Inl("DIGITS")},
                {syntax.Inl("INTEGER_VALUE")},
                {syntax.Inl("REAL")},
            },
        };
    }

    template <>
    THumanHighlighting::TUnit MakeUnit<EHUnitKind::StringLiteral>(Syntax& syntax) {
        return {
            .Kind = EHUnitKind::StringLiteral,
            .Tokens = {
                {syntax.Inl("STRING_VALUE")},
            },
        };
    }

    template <>
    THumanHighlighting::TUnit MakeUnit<EHUnitKind::Comment>(Syntax& syntax) {
        return {
            .Kind = EHUnitKind::Comment,
            .Tokens = {
                {syntax.Inl("COMMENT")},
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

    THumanHighlighting MakeHighlighting(NSQLReflect::TLexerGrammar grammar) {
        Syntax syntax = MakeSyntax(std::move(grammar));

        THumanHighlighting h;
        h.Units.emplace_back(MakeUnit<EHUnitKind::Keyword>(syntax));
        h.Units.emplace_back(MakeUnit<EHUnitKind::Punctuation>(syntax));
        h.Units.emplace_back(MakeUnit<EHUnitKind::Identifier>(syntax));
        h.Units.emplace_back(MakeUnit<EHUnitKind::QuotedIdentifier>(syntax));
        h.Units.emplace_back(MakeUnit<EHUnitKind::BindParamterIdentifier>(syntax));
        h.Units.emplace_back(MakeUnit<EHUnitKind::TypeIdentifier>(syntax));
        h.Units.emplace_back(MakeUnit<EHUnitKind::FunctionIdentifier>(syntax));
        h.Units.emplace_back(MakeUnit<EHUnitKind::Literal>(syntax));
        h.Units.emplace_back(MakeUnit<EHUnitKind::StringLiteral>(syntax));
        h.Units.emplace_back(MakeUnit<EHUnitKind::Comment>(syntax));

        syntax.Ref("WS");
        for (auto& referenced : syntax.Referenced) {
            TString content = syntax.Inl(referenced);
            h.References.emplace(std::move(referenced), std::move(content));
        }

        return h;
    }

} // namespace SQLHighlight

template <>
void Out<SQLHighlight::EHUnitKind>(IOutputStream& out, SQLHighlight::EHUnitKind kind) {
    switch (kind) {
        case SQLHighlight::Keyword:
            out << "Keyword";
            break;
        case SQLHighlight::Punctuation:
            out << "Punctuation";
            break;
        case SQLHighlight::Identifier:
            out << "Identifier";
            break;
        case SQLHighlight::QuotedIdentifier:
            out << "QuotedIdentifier";
            break;
        case SQLHighlight::BindParamterIdentifier:
            out << "BindParamterIdentifier";
            break;
        case SQLHighlight::TypeIdentifier:
            out << "TypeIdentifier";
            break;
        case SQLHighlight::FunctionIdentifier:
            out << "FunctionIdentifier";
            break;
        case SQLHighlight::Literal:
            out << "Literal";
            break;
        case SQLHighlight::StringLiteral:
            out << "StringLiteral";
            break;
        case SQLHighlight::Comment:
            out << "Comment";
            break;
    }
}
