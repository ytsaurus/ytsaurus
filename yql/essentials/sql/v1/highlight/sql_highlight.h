#pragma once

#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>

namespace NSQLHighlight {

    enum class EUnitKind {
        Keyword,
        Punctuation,
        Identifier,
        QuotedIdentifier,
        BindParamterIdentifier,
        TypeIdentifier,
        FunctionIdentifier,
        Literal,
        StringLiteral,
        Comment,
    };

    struct TPattern {
        TString BodyRe;
        TString AfterRe = "";

        bool IsPlain() const;
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
