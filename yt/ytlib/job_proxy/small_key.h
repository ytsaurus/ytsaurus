#pragma once

#include "public.h"
#include <ytlib/table_client/key.h>
#include <ytlib/table_client/public.h>
#include <ytlib/ytree/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TSmallKeyPart
{
    NTableClient::EKeyPartType Type;
    ui32 Length;

    union {
        i64 Int;
        double Double;
        const char* Str;
    } Value;

    TStringBuf GetString() const
    {
        return TStringBuf(Value.Str, Value.Str + Length);
    }

    TSmallKeyPart() 
        : Type(NTableClient::EKeyPartType::Null)
    { }
};

void SetSmallKeyPart(TSmallKeyPart& keyPart, const TStringBuf& yson, NYTree::TLexer& lexer);
int CompareSmallKeyParts(const TSmallKeyPart& lhs, const TSmallKeyPart& rhs);
void SetKeyPart(NTableClient::TNonOwningKey* key, const TSmallKeyPart& keyPart, int keyIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
