#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NTableClient {

using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

const NLog::TLogger TableClientLogger("TableClient");

////////////////////////////////////////////////////////////////////////////////

TUnversionedValue MakeKeyPart(const TStringBuf& yson, NYson::TStatelessLexer& lexer)
{
    NYson::TToken token;
    lexer.GetToken(yson, &token);
    YCHECK(!token.IsEmpty());

    switch (token.GetType()) {
        case NYson::ETokenType::Int64:
            return MakeUnversionedInt64Value(token.GetInt64Value());

        case NYson::ETokenType::Uint64:
            return MakeUnversionedUint64Value(token.GetUint64Value());

        case NYson::ETokenType::Double:
            return MakeUnversionedDoubleValue(token.GetDoubleValue());

        case NYson::ETokenType::Boolean:
            return MakeUnversionedBooleanValue(token.GetBooleanValue());

        case NYson::ETokenType::String:
            return MakeUnversionedStringValue(token.GetStringValue());

        default:
            return MakeUnversionedAnyValue(yson);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

