#include "token.h"

#include <yt/core/misc/error.h>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

const TStringBuf ListBeginToken("begin");
const TStringBuf ListEndToken("end");
const TStringBuf ListBeforeToken("before:");
const TStringBuf ListAfterToken("after:");

TStringBuf ExtractListIndex(TStringBuf token)
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

int ParseListIndex(TStringBuf token)
{
    try {
        return FromString<int>(token);
    } catch (const std::exception&) {
        THROW_ERROR_EXCEPTION("Invalid list index: %v",
            token);
    }
}

TString ToYPathLiteral(TStringBuf value)
{
    //! Keep it synchronized with the same functions in python, C++ and other APIs.
    static const char* HexChars = "0123456789abcdef";
    TString result;
    result.reserve(value.length() + 16);
    for (unsigned char ch : value) {
        if (ch == '\\' || ch == '/' || ch == '@' || ch == '*' || ch == '&' || ch == '[' || ch == '{') {
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

TString ToYPathLiteral(i64 value)
{
    return ToString(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
