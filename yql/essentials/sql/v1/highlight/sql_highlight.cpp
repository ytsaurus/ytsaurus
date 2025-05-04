#include "sql_highlight.h"

#include <yql/essentials/sql/v1/lexer/regex/regex.h>

#include <contrib/libs/re2/re2/re2.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NSQLHighlight {

    struct Syntax {
        const NSQLReflect::TLexerGrammar* Grammar;
        THashMap<TString, TString> RegexesDefault;
        THashMap<TString, TString> RegexesANSI;

        TString Concat(const TVector<TStringBuf>& names) {
            TString concat;
            for (const auto& name : names) {
                concat += Get(name);
            }
            return concat;
        }

        TString Get(const TStringBuf name, bool ansi = false) const {
            if (Grammar->PunctuationNames.contains(name)) {
                return RE2::QuoteMeta(Grammar->BlockByName.at(name));
            }
            if (ansi) {
                return RegexesANSI.at(name);
            }
            return RegexesDefault.at(name);
        }
    };

    NSQLTranslationV1::TRegexPattern CaseInsensitive(TStringBuf text) {
        return {
            .Body = TString(text),
            .IsCaseInsensitive = true,
        };
    }

    template <EUnitKind K>
    TUnit MakeUnit(Syntax& syntax);

    template <>
    TUnit MakeUnit<EUnitKind::Keyword>(Syntax& s) {
        using NSQLReflect::TLexerGrammar;

        TUnit unit = {.Kind = EUnitKind::Keyword};
        for (const auto& keyword : s.Grammar->KeywordNames) {
            const TStringBuf content = TLexerGrammar::KeywordBlock(keyword);
            unit.Patterns.push_back(CaseInsensitive(content));
        }
        return unit;
    }

    template <>
    TUnit MakeUnit<EUnitKind::Punctuation>(Syntax& s) {
        TUnit unit = {.Kind = EUnitKind::Punctuation};
        for (const auto& name : s.Grammar->PunctuationNames) {
            const TString content = s.Get(name);
            unit.Patterns.push_back({content});
        }
        return unit;
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
                CaseInsensitive("Decimal"),
                CaseInsensitive("Bool"),
                CaseInsensitive("Int8"),
                CaseInsensitive("Int16"),
                CaseInsensitive("Int32"),
                CaseInsensitive("Int64"),
                CaseInsensitive("Uint8"),
                CaseInsensitive("Uint16"),
                CaseInsensitive("Uint32"),
                CaseInsensitive("Uint64"),
                CaseInsensitive("Float"),
                CaseInsensitive("Double"),
                CaseInsensitive("DyNumber"),
                CaseInsensitive("String"),
                CaseInsensitive("Utf8"),
                CaseInsensitive("Json"),
                CaseInsensitive("JsonDocument"),
                CaseInsensitive("Yson"),
                CaseInsensitive("Uuid"),
                CaseInsensitive("Date"),
                CaseInsensitive("Datetime"),
                CaseInsensitive("Timestamp"),
                CaseInsensitive("Interval"),
                CaseInsensitive("TzDate"),
                CaseInsensitive("TzDateTime"),
                CaseInsensitive("TzTimestamp"),
                CaseInsensitive("Callable"),
                CaseInsensitive("Resource"),
                CaseInsensitive("Tagged"),
                CaseInsensitive("Generic"),
                CaseInsensitive("Unit"),
                CaseInsensitive("Null"),
                CaseInsensitive("Void"),
                CaseInsensitive("EmptyList"),
                CaseInsensitive("EmptyDict"),
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
    TUnit MakeUnit<EUnitKind::Identifier>(Syntax& s) {
        return {
            .Kind = EUnitKind::Identifier,
            .Patterns = {
                {s.Get("ID_PLAIN")},
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
            .Patterns = {{s.Get("STRING_VALUE")}},
            .PatternsANSIOverride = {{s.Get("STRING_VALUE", /* ansi = */ true)}},
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::Comment>(Syntax& s) {
        return {
            .Kind = EUnitKind::Comment,
            .Patterns = {{s.Get("COMMENT")}},
            .PatternsANSIOverride = {},
        };
    }

    template <>
    TUnit MakeUnit<EUnitKind::Whitespace>(Syntax& s) {
        return {
            .Kind = EUnitKind::Whitespace,
            .Patterns = {
                {s.Get("WS")},
            },
        };
    }

    Syntax MakeSyntax(const NSQLReflect::TLexerGrammar& grammar) {
        using NSQLTranslationV1::MakeRegexByOtherName;

        Syntax syntax;
        syntax.Grammar = &grammar;
        for (auto& [k, v] : MakeRegexByOtherName(*syntax.Grammar, /* ansi = */ false)) {
            syntax.RegexesDefault.emplace(std::move(k), std::move(v));
        }
        for (auto& [k, v] : MakeRegexByOtherName(*syntax.Grammar, /* ansi = */ true)) {
            syntax.RegexesANSI.emplace(std::move(k), std::move(v));
        }
        return syntax;
    }

    THighlighting MakeHighlighting() {
        return MakeHighlighting(NSQLReflect::LoadLexerGrammar());
    }

    THighlighting MakeHighlighting(const NSQLReflect::TLexerGrammar& grammar) {
        Syntax s = MakeSyntax(grammar);

        THighlighting h;
        h.Units.emplace_back(MakeUnit<EUnitKind::Keyword>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::Punctuation>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::QuotedIdentifier>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::BindParamterIdentifier>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::TypeIdentifier>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::FunctionIdentifier>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::Identifier>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::Literal>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::StringLiteral>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::Comment>(s));
        h.Units.emplace_back(MakeUnit<EUnitKind::Whitespace>(s));

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
        case NSQLHighlight::EUnitKind::Identifier:
            out << "Identifier";
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
        case NSQLHighlight::EUnitKind::Whitespace:
            out << "Ws";
            break;
        case NSQLHighlight::EUnitKind::Error:
            out << "Error";
            break;
    }
}
