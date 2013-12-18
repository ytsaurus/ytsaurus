#pragma once

#include <core/logging/log.h>
#include <core/yson/lexer.h>

#include <ytlib/chunk_client/schema.pb.h>

#include <ytlib/new_table_client/unversioned_row.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger TableReaderLogger;
extern NLog::TLogger TableWriterLogger;

extern const int FormatVersion;

extern const size_t MaxKeySize;

NVersionedTableClient::TUnversionedValue MakeKeyPart(const TStringBuf& yson, NYson::TStatelessLexer& lexer);

////////////////////////////////////////////////////////////////////////////////

// XXX(psushin): some legacy staff, compat with old-style keys.

DECLARE_ENUM(EKeyPartType,
    // A special sentinel used by #GetKeySuccessor.
    ((MinSentinel)(-1))
    // Denotes a missing (null) component in a composite key.
    ((Null)(0))
    // Integer value.
    ((Integer)(1))
    // Floating-point value.
    ((Double)(2))
    // String value.
    ((String)(3))
    // Any structured value.
    ((Composite)(4))

    // A special sentinel used by #GetKeyPrefixSuccessor.
    ((MaxSentinel)(100))
);

NChunkClient::NProto::TKey GetKeySuccessor(const NChunkClient::NProto::TKey& key);

int CompareKeys(
    const NChunkClient::NProto::TKey& lhs, 
    const NChunkClient::NProto::TKey& rhs, 
    int prefixLength = std::numeric_limits<int>::max());

////////////////////////////////////////////////////////////////////////////////

template<class TVersionedRow>
void ToProto(NChunkClient::NProto::TKey* protoKey, const TVersionedRow& row)
{
    protoKey->clear_parts();
    for (int i = 0; i < row.GetValueCount(); ++i) {
        auto* keyPart = protoKey->add_parts();
        switch (row[i].Type) {
        case NVersionedTableClient::EValueType::Null:
            keyPart->set_type(EKeyPartType::Null);
            break;

        case NVersionedTableClient::EValueType::Integer:
            keyPart->set_type(EKeyPartType::Integer);
            keyPart->set_int_value(row[i].Data.Integer);
            break;

        case NVersionedTableClient::EValueType::Double:
            keyPart->set_type(EKeyPartType::Double);
            keyPart->set_double_value(row[i].Data.Double);
            break;

        case NVersionedTableClient::EValueType::String:
            keyPart->set_type(EKeyPartType::String);
            keyPart->set_str_value(row[i].Data.String, row[i].Length);
            break;

        case NVersionedTableClient::EValueType::Any:
            keyPart->set_type(EKeyPartType::Composite);
            break;

        default:
            YUNREACHABLE();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

