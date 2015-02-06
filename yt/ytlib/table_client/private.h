#pragma once

#include <core/logging/log.h>
#include <core/yson/lexer.h>

#include <ytlib/chunk_client/schema.pb.h>

#include <ytlib/new_table_client/unversioned_row.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger TableClientLogger;

NVersionedTableClient::TUnversionedValue MakeKeyPart(const TStringBuf& yson, NYson::TStatelessLexer& lexer);

////////////////////////////////////////////////////////////////////////////////

// COMPAT(psushin): some legacy staff, compat with old-style keys.

DEFINE_ENUM(EKeyPartType,
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
    // Boolean value.
    ((Boolean)(5))
    // Uint64 value.
    ((Uint64)(6))

    // A special sentinel used by #GetKeyPrefixSuccessor.
    ((MaxSentinel)(100))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

