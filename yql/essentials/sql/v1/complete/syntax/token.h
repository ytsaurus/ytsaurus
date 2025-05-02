#pragma once

#include <yql/essentials/sql/v1/complete/core/input.h>
#include <yql/essentials/sql/v1/complete/text/word.h>

#include <yql/essentials/parser/lexer_common/lexer.h>

namespace NSQLComplete {

    using NSQLTranslation::ILexer;
    using NSQLTranslation::TParsedToken;
    using NSQLTranslation::TParsedTokenList;

    struct TCursor {
        size_t PrevTokenIndex = 0;
        size_t NextTokenIndex = PrevTokenIndex;
        size_t Position = 0;

        bool IsEnclosed() const;
    };

    struct TRichParsedToken {
        const TParsedToken* Base;
        size_t Index;
        size_t Position;

        bool IsLiteral() const;
    };

    struct TCursorTokenContext {
        TParsedTokenList Tokens;
        TVector<size_t> TokenPositions;
        TCursor Cursor;

        TRichParsedToken TokenAt(size_t index) const;
        TMaybe<TRichParsedToken> Enclosing() const;
        size_t PositionWithinEnclosing() const;
        TMaybe<TRichParsedToken> MatchCursorPrefix(const TVector<TStringBuf>& pattern) const;
    };

    bool GetStatement(
        ILexer::TPtr& lexer,
        TCompletionInput input,
        TCompletionInput& output,
        size_t& output_position);

    bool GetCursorTokenContext(ILexer::TPtr& lexer, TCompletionInput input, TCursorTokenContext& context);

} // namespace NSQLComplete
