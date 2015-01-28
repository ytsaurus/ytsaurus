#include "stdafx.h"
#include "token.h"

#include <core/misc/error.h>

namespace NYT {
namespace NYPath {

////////////////////////////////////////////////////////////////////////////////

const TStringBuf ListBeginToken("begin");
const TStringBuf ListEndToken("end");
const TStringBuf ListBeforeToken("before:");
const TStringBuf ListAfterToken("after:");

TStringBuf ExtractListIndex(const TStringBuf& token)
{
    if (token[0] >= '0' && token[0] <= '9') {
        return token;
    } else {
        int colonIndex = token.find(':');
        if (colonIndex == TStringBuf::npos) {
            return token;
        } else {
            return TStringBuf(token.begin() + colonIndex + 1, token.end());
        }
    }
}

int ParseListIndex(const TStringBuf& token)
{
    try {
        return FromString<int>(token);
    } catch (const std::exception&) {
        THROW_ERROR_EXCEPTION("Invalid list index: %v",
            token);
    }
}

Stroka ToYPathLiteral(const TStringBuf& value)
{
    static const char* HexChars = "0123456789abcdef";
    Stroka result;
    result.reserve(value.length() + 16);
    for (char ch : value) {
        if (ch == '\\' || ch == '/' || ch == '@' || ch == '&' || ch == '[' || ch == '{') {
            result.append('\\');
            result.append(ch);
        } else if (ch < 32 || ch > 127) {
            result.append('\\');
            result.append('x');
            result.append(HexChars[ch >> 4]);
            result.append(HexChars[ch & 15]);
        } else {
            result.append(ch);
        }
    }
    return result;
}

Stroka ToYPathLiteral(i64 value)
{
    return ToString(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYPath
} // namespace NYT
