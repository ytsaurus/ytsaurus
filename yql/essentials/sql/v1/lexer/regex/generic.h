#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>

#include <functional>

namespace NSQLTranslationV1 {

    struct TGenericToken {
        static constexpr const char* Error = "ERROR";

        TStringBuf Name;
        TStringBuf Content;
        size_t Begin = 0; // In bytes
    };

    class IGenericLexer: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IGenericLexer>;
        using TTokenCallback = std::function<void(TGenericToken&& token)>;
        using TMatcher = std::function<TMaybe<TStringBuf>(TStringBuf prefix)>;

        struct TTokenMatcher {
            TString TokenName;
            TMatcher Match;
        };

        using TGrammar = TVector<TTokenMatcher>;

        static constexpr size_t MaxErrorsLimit = std::numeric_limits<size_t>::max();

        virtual ~IGenericLexer() = default;
        virtual void Tokenize(
            TStringBuf text,
            const TTokenCallback& onNext,
            size_t maxErrors = IGenericLexer::MaxErrorsLimit) const = 0;
    };

    struct TRegexPattern {
        TString Body;
        TString After = "";
        bool IsCaseInsensitive = false;
        bool IsLongestMatch = true;
    };

    IGenericLexer::TMatcher Compile(const TRegexPattern& regex);

    IGenericLexer::TPtr MakeGenericLexer(IGenericLexer::TGrammar grammar);

    TVector<TGenericToken> Tokenize(IGenericLexer::TPtr& lexer, TStringBuf text);

} // namespace NSQLTranslationV1
