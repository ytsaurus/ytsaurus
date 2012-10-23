#include "helpers.h"

#include <ytlib/misc/error.h>

#include <ytlib/ytree/yson_format.h>
#include <ytlib/ytree/yson_string.h>
#include <ytlib/ytree/token.h>

namespace NYT {

using namespace NYTree;

namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

void TFormatsConsumerBase::OnRaw(const TStringBuf& yson, EYsonType type)
{
    // OnRaw is supported only on nodes
    if (type != EYsonType::Node) {
        YUNIMPLEMENTED();
    }

    // For peformance reason try to consume only one token first
    Lexer.Reset();
    Lexer.Read(yson);
    Lexer.Finish();

    YCHECK(Lexer.GetState() == TLexer::EState::Terminal);
    auto token = Lexer.GetToken();
    switch(token.GetType()) {
        case ETokenType::String:
            OnStringScalar(token.GetStringValue());
            break;

        case ETokenType::Integer:
            OnIntegerScalar(token.GetIntegerValue());
            break;

        case ETokenType::Double:
            OnDoubleScalar(token.GetDoubleValue());
            break;

        case EntityToken:
        case BeginListToken:
        case BeginMapToken:
        case BeginAttributesToken:
            // Fallback to usual OnRaw
            TYsonConsumerBase::OnRaw(yson, type);
            break;

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
