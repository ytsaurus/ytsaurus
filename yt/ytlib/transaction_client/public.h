#pragma once

#include <core/misc/common.h>
#include <core/misc/enum.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ETransactionType,
    (Master) // accepted by both masters and tablets
    (Tablet) // accepted by tablets only
);

DECLARE_REFCOUNTED_CLASS(TTransaction)
DECLARE_REFCOUNTED_CLASS(TTransactionManager)

DECLARE_REFCOUNTED_CLASS(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

typedef ui64 TTimestamp;

// Uninitialized/invalid timestamp.
const TTimestamp NullTimestamp = 0;

// Valid timestamps.
const TTimestamp MinTimestamp = 0x0000000000000001ULL;
const TTimestamp MaxTimestamp = 0x0fffffffffffffffULL;

// Sentinels.
const TTimestamp LastCommittedTimestamp = 0x1000000000000000ULL;
const TTimestamp UncommittedTimestamp   = 0x1000000000000001ULL;
const TTimestamp AllCommittedTimestamp  = 0x1000000000000002ULL;

// Masks.
const TTimestamp TimestampValueMask       = 0x3fffffffffffffffULL;
const TTimestamp TombstoneTimestampMask   = 0x8000000000000000ULL;
const TTimestamp IncrementalTimestampMask = 0x4000000000000000ULL;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
