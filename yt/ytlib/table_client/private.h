#pragma once

#include <core/logging/log.h>
#include <core/yson/lexer.h>

#include <ytlib/chunk_client/schema.pb.h>

#include <ytlib/new_table_client/unversioned_row.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger TableClientLogger;

NVersionedTableClient::TUnversionedValue MakeKeyPart(const TStringBuf& yson, NYson::TStatelessLexer& lexer);

////////////////////////////////////////////////////////////////////////////////

// COMPAT(psushin): some legacy staff, compat with old-style keys.

DECLARE_ENUM(EKeyPartType,
    // A special sentinel used by #GetKeySuccessor.
    ((MinSentinel)(-1))
    // Denotes a missing (null) component in a composite key.
    ((Null)(0))
    // Int64 value.
    ((Int64)(1))
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

void ToProto(
    NChunkClient::NProto::TKey* protoKey,
    NVersionedTableClient::TUnversionedRow row);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

