#pragma once

#include "sql_highlight.h"

#include <util/generic/ptr.h>

#include <functional>

namespace NSQLHighlight {

    struct TToken {
        EUnitKind Kind;
        size_t Begin;  // In bytes
        size_t Length; // In bytes
    };

    class IHighlighter: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IHighlighter>;
        using TTokenCallback = std::function<void(TToken&& token)>; // TODO: return bool to intr

        virtual ~IHighlighter() = default;
        virtual void Tokenize(TStringBuf text, const TTokenCallback& onNext) const = 0; // TODO: return false if failed
    };

    TVector<TToken> Tokenize(IHighlighter& highlighter, TStringBuf text);

    IHighlighter::TPtr MakeHighlighter(const THighlighting& highlighting);

} // namespace NSQLHighlight
