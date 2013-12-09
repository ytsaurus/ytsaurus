#include "stdafx.h"

#include "small_key.h"

#include <core/yson/lexer.h>

namespace NYT {
namespace NJobProxy {

using namespace NYTree;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

void SetSmallKeyPart(TSmallKeyPart& keyPart, const TStringBuf& yson, NYson::TStatelessLexer& lexer)
{
    NYson::TToken token;
    lexer.GetToken(yson, &token);
    YCHECK(!token.IsEmpty());

    switch (token.GetType()) {
    case NYson::ETokenType::Integer:
        keyPart.Type = EValueType::Integer;
        keyPart.Value.Int = token.GetIntegerValue();
        break;

    case NYson::ETokenType::Double:
        keyPart.Type = EValueType::Double;
        keyPart.Value.Double = token.GetDoubleValue();
        break;

    case NYson::ETokenType::String: {
        keyPart.Type = EValueType::String;
        auto& value = token.GetStringValue();
        keyPart.Value.Str = ~value;
        keyPart.Length = static_cast<ui32>(value.size());
        break;
    }

    default:
        keyPart.Type = EValueType::Any;
        break;
    }
}

int CompareSmallKeyParts(const TSmallKeyPart& lhs, const TSmallKeyPart& rhs)
{
    if (lhs.Type != rhs.Type) {
        return static_cast<int>(lhs.Type) - static_cast<int>(rhs.Type);
    }

    switch (lhs.Type) {
    case EValueType::Integer:
        if (lhs.Value.Int > rhs.Value.Int)
            return 1;
        if (lhs.Value.Int < rhs.Value.Int)
            return -1;
        return 0;

    case EValueType::Double:
        if (lhs.Value.Double > rhs.Value.Double)
            return 1;
        if (lhs.Value.Double < rhs.Value.Double)
            return -1;
        return 0;

    case EValueType::String:
        return lhs.GetString().compare(rhs.GetString());

    case EValueType::Any:
    case EValueType::Null:
        return 0;

    default:
        YUNREACHABLE();
    }

    YUNREACHABLE();
}

TUnversionedValue MakeKeyPart(const TSmallKeyPart& keyPart)
{
    switch (keyPart.Type) {
    case EValueType::Integer:
        return MakeUnversionedIntegerValue(keyPart.Value.Int);

    case EValueType::Double:
        return MakeUnversionedDoubleValue(keyPart.Value.Double);

    case EValueType::String:
        return MakeUnversionedStringValue(keyPart.GetString());

    case EValueType::Null:
        return MakeUnversionedSentinelValue(EValueType::Null);

    case EValueType::Any:
        return MakeUnversionedAnyValue(TStringBuf());

    default:
        YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
