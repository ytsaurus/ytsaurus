#include "stdafx.h"
#include "convert.h"

#include <core/yson/tokenizer.h>

namespace NYT {
namespace NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

template TYsonString ConvertToYsonString<int>(const int&);
template TYsonString ConvertToYsonString<long>(const long&);
template TYsonString ConvertToYsonString<unsigned long>(const unsigned long&);
template TYsonString ConvertToYsonString<Stroka>(const Stroka&);

TYsonString ConvertToYsonString(const char* value)
{
    return ConvertToYsonString(Stroka(value));
}

const TToken& SkipAttributes(TTokenizer* tokenizer)
{
    int depth = 0;
    while (true) {
        tokenizer->ParseNext();
        const auto& token = tokenizer->CurrentToken();
        switch (token.GetType()) {
            case ETokenType::LeftBrace:
            case ETokenType::LeftAngle:
                ++depth;
                break;

            case ETokenType::RightBrace:
            case ETokenType::RightAngle:
                --depth;
                break;

            default:
                if (depth == 0) {
                    return token;
                }
                break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

