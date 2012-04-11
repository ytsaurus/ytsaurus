#include "stdafx.h"
#include "token.h"

//#include <util/string/cast.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

const TToken TToken::EndOfStream;

TToken::TToken()
    : IntegerValue(0)
    , DoubleValue(0.0)
{ }

bool TToken::IsEmpty() const
{
    return Type_ == ETokenType::None;
}

const Stroka& TToken::GetStringValue() const
{
    YASSERT(Type_ == ETokenType::String);
    return StringValue;
}

i64 TToken::GetIntegerValue() const
{
    YASSERT(Type_ == ETokenType::Integer);
    return IntegerValue;
}

double TToken::GetDoubleValue() const
{
    YASSERT(Type_ == ETokenType::Double);
    return DoubleValue;
}

Stroka TToken::ToString() const
{
    switch (Type_) {
        case ETokenType::Integer:
            return ::ToString(IntegerValue);

        case ETokenType::Double:
            return ::ToString(DoubleValue);

        default:
            return StringValue;
    }
}

const TToken& TToken::CheckType(ETokenType expectedType) const
{
    if (Type_ != expectedType) {
        ythrow yexception() << Sprintf("Unexpected token (Token: %s, TokenType: %s, ExpectedType: %s)",
            ~ToString().Quote(),
            ~Type_.ToString(),
            ~expectedType.ToString());
    }
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

void ThrowUnexpectedToken(const TToken& token)
{
    ythrow yexception() << Sprintf("Unexpected token (Token: %s, TokenType: %s)",
        ~token.ToString().Quote(),
        ~token.GetType().ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
