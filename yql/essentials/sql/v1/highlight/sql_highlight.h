#pragma once

#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>

namespace NSQLHighlight {

    enum class EUnitKind {
        Keyword,
        Punctuation,
        QuotedIdentifier,
        BindParamterIdentifier,
        TypeIdentifier,
        FunctionIdentifier,
        Identifier,
        Literal,
        StringLiteral,
        Comment,
        Whitespace,
    };

    // TODO: abstract TRegex = (Re, IsCaseInsensitive)
    struct TPattern {
        TString BodyRe;
        TString AfterRe = "";
        bool IsCaseInsensitive = false;
        bool IsLongestMatch = true;
    };

    struct TUnit {
        EUnitKind Kind;
        TVector<TPattern> Patterns;
    };

    struct THighlighting {
        TVector<TUnit> Units;
        TPattern Whitespace;
    };

    THighlighting MakeHighlighting(NSQLReflect::TLexerGrammar grammar);

} // namespace NSQLHighlight
