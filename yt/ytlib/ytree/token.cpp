#include "stdafx.h"
#include "token.h"

//#include <util/string/cast.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TToken::TToken()
    : Int64Value(0)
    , DoubleValue(0.0)
{ }

const Stroka& TToken::GetStringValue() const
{
    YASSERT(Type_ == ETokenType::String);
    return StringValue;
}

i64 TToken::GetInt64Value() const
{
    YASSERT(Type_ == ETokenType::Int64);
    return Int64Value;
}

double TToken::GetDoubleValue() const
{
    YASSERT(Type_ == ETokenType::Double);
    return DoubleValue;
}

Stroka TToken::ToString() const
{
    switch (Type_) {
        case ETokenType::Int64:
            return ::ToString(Int64Value);

        case ETokenType::Double:
            return ::ToString(DoubleValue);

        default:
            return StringValue;
    }
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYtree
} // namespace NYT
