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

    // TODO: Introfuce the GenericLexer powered by TPattern.
    //
    //       Use it for both sql/v1/lexer/regex and sql/v1/highlight.
    //       - /sql/v1/lexer/regex/generic.h
    //       - /sql/v1/lexer/regex/lexer.h
    //
    //       Translatation. Grammar -> Highlighting -> GenericLexerGrammar.
    //
    //       GenericLexer:
    //         Uses [{TokenKind: TString, Matcher}] for matching
    //         Matcher = FunctionRef<TMaybe<TStringBuf>(TStringBuf prefix)>
    //         RegexMatcher(TPattern) -> Matcher
    //         TToken = { Kind: TStringBuf, Begin: size_t, size_t Length }
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
