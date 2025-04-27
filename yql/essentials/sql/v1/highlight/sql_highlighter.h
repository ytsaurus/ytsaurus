#pragma once

#include "sql_highlight.h"

#include <util/generic/ptr.h>
#include <util/generic/function_ref.h>

namespace NSQLHighlight {

    struct TToken {
        EUnitKind Kind;
        size_t Begin;
        size_t Length;
    };

    class IHighlighter: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IHighlighter>;
        using TTokenCallback = TFunctionRef<void(TToken&& token)>;

        virtual ~IHighlighter() = default;
        virtual void Tokenize(TStringBuf text, const TTokenCallback& onNext) const = 0;
    };

    TVector<TToken> Tokenize(IHighlighter& highlighter, TStringBuf text);

    IHighlighter::TPtr MakeHighlighter(THighlighting highlighting);

} // namespace NSQLHighlight
