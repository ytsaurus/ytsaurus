#include "stdafx.h"

#include "small_key.h"

#include <ytlib/ytree/lexer.h>

namespace NYT {
namespace NJobProxy {

using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void SetSmallKeyPart(TSmallKeyPart& keyPart, const TStringBuf& yson, TLexer& lexer)
{
    lexer.Reset();
    YCHECK(lexer.Read(yson) > 0);
    YASSERT(lexer.GetState() == NYTree::TLexer::EState::Terminal);

    const auto& token = lexer.GetToken();
    switch (token.GetType()) {
    case ETokenType::Integer:
        keyPart.Type = EKeyPartType::Integer;
        keyPart.Value.Int = token.GetIntegerValue();
        break;

    case ETokenType::Double:
        keyPart.Type = EKeyPartType::Double;
        keyPart.Value.Double = token.GetDoubleValue();
        break;

    case ETokenType::String: {
        keyPart.Type = EKeyPartType::String;
        auto& value = token.GetStringValue();
        keyPart.Value.Str = ~value;
        keyPart.Length = static_cast<ui32>(value.size());
        break;
    }

    default:
        keyPart.Type = EKeyPartType::Composite;
        break;
    }
}

int CompareSmallKeyParts(const TSmallKeyPart& lhs, const TSmallKeyPart& rhs)
{
    if (lhs.Type != rhs.Type) {
        return static_cast<int>(lhs.Type) - static_cast<int>(rhs.Type);
    }

    switch (lhs.Type) {
    case EKeyPartType::Integer:
        if (lhs.Value.Int > rhs.Value.Int)
            return 1;
        if (lhs.Value.Int < rhs.Value.Int)
            return -1;
        return 0;

    case EKeyPartType::Double:
        if (lhs.Value.Double > rhs.Value.Double)
            return 1;
        if (lhs.Value.Double < rhs.Value.Double)
            return -1;
        return 0;

    case EKeyPartType::String:
        return lhs.GetString().compare(rhs.GetString());

    case EKeyPartType::Composite:
    case EKeyPartType::Null:
        return 0;

    default:
        YUNREACHABLE();
    }

    YUNREACHABLE();
}


void SetKeyPart(TNonOwningKey* key, const TSmallKeyPart& keyPart, int keyIndex)
{
    switch (keyPart.Type) {
    case EKeyPartType::Integer:
        key->SetValue(keyIndex, keyPart.Value.Int);
        break;

    case EKeyPartType::Double:
        key->SetValue(keyIndex, keyPart.Value.Double);
        break;

    case EKeyPartType::String:
        key->SetValue(keyIndex, keyPart.GetString());
        break;

    case EKeyPartType::Null:
    case EKeyPartType::Composite:
        key->SetSentinel(keyIndex, keyPart.Type);
        break;

    default:
        YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
