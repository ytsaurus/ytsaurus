#pragma once

#include "public.h"
#include <ytlib/chunk_client/public.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <core/yson/lexer.h>
#include <core/ytree/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TSmallKeyPart
{
    NVersionedTableClient::EValueType Type;
    ui32 Length;

    union {
        i64 Int64;
        ui64 Uint64;
        double Double;
        bool Boolean;
        const char* Str;
    } Value;

    TStringBuf GetString() const
    {
        return TStringBuf(Value.Str, Value.Str + Length);
    }

    TSmallKeyPart()
        : Type(NVersionedTableClient::EValueType::Null)
    { }
};

void SetSmallKeyPart(TSmallKeyPart& keyPart, const TStringBuf& yson, NYson::TStatelessLexer& lexer);
int CompareSmallKeyParts(const TSmallKeyPart& lhs, const TSmallKeyPart& rhs);
NVersionedTableClient::TUnversionedValue MakeKeyPart(const TSmallKeyPart& keyPart);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
