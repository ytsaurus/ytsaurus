#pragma once

#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>

namespace NSQLHighlight {

    using TMetaToken = TVector<TString>;

    enum EHUnitKind {
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

    struct THumanHighlighting {
        struct TUnit {
            EHUnitKind Kind;
            TVector<TMetaToken> Tokens;
        };

        TVector<TUnit> Units;
        TMap<TString, TString> References;
    };

    struct TMachineHighlighting {
        struct TUnit {
            EHUnitKind Kind;
            TString Regex;
        };

        TVector<TUnit> Units;
    };

    THumanHighlighting MakeHighlighting(NSQLReflect::TLexerGrammar grammar);

} // namespace NSQLHighlight
