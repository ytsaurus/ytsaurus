#pragma once

#include "public.h"
#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/key.h>
#include <core/ytree/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TSmallKeyPart
{
    NChunkClient::EKeyPartType Type;
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
        : Type(NChunkClient::EKeyPartType::Null)
    { }
};

void SetSmallKeyPart(TSmallKeyPart& keyPart, const TStringBuf& yson, NYson::TStatelessLexer& lexer);
int CompareSmallKeyParts(const TSmallKeyPart& lhs, const TSmallKeyPart& rhs);
void SetKeyPart(NChunkClient::TNonOwningKey* key, const TSmallKeyPart& keyPart, int keyIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
