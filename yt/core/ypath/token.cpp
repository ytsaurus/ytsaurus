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
        THROW_ERROR_EXCEPTION("Invalid list index %Qv",
            token);
    }
}

TString ToYPathLiteral(TStringBuf value)
{
    TStringBuilder builder;
    AppendYPathLiteral(&builder, value);
    return builder.Flush();
}

TString ToYPathLiteral(i64 value)
{
    return ToString(value);
}

void AppendYPathLiteral(TStringBuilderBase* builder, TStringBuf value)
{
    builder->Preallocate(value.length() + 16);
    for (unsigned char ch : value) {
        if (ch == '\\' || ch == '/' || ch == '@' || ch == '*' || ch == '&' || ch == '[' || ch == '{') {
            builder->AppendChar('\\');
            builder->AppendChar(ch);
        } else if (ch < 32 || ch > 127) {
            builder->AppendString(AsStringBuf("\\x"));
            builder->AppendChar(Int2Hex[ch >> 4]);
            builder->AppendChar(Int2Hex[ch & 0xf]);
        } else {
            builder->AppendChar(ch);
        }
    }
}

void AppendYPathLiteral(TStringBuilderBase* builder, i64 value)
{
    builder->AppendFormat("%v", value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
