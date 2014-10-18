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

DECLARE_REFCOUNTED_STRUCT(ITimestampProvider)

DECLARE_REFCOUNTED_CLASS(TTransactionManagerConfig)
DECLARE_REFCOUNTED_CLASS(TRemoteTimestampProviderConfig)

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TTransactionId;
using NObjectClient::NullTransactionId;

//! Timestamp is a cluster-wide unique monotonically increasing number
//! used for implementing MVCC paradigm.
/*!
 *  Timestamp is represented as a 64-bit unsigned integer of the following structure:
 *  bits  0-29:  auto-incrementing counter (allowing up to ~10^9 timestamps per second)
 *  bits 30-61:  Unix time in seconds (from 1 Jan 1970)
 *  bits 62-63:  reserved
 */
typedef ui64 TTimestamp;

// Number of bits in the counter part.
const int TimestampCounterWidth = 30;

// Uninitialized/invalid timestamp.
const TTimestamp NullTimestamp = 0;

// Timestamp values range.
const TTimestamp MinTimestamp = 0x0000000000000001ULL;
const TTimestamp MaxTimestamp = 0x3fffffffffffff00ULL;

// Sentinels.
const TTimestamp SyncLastCommittedTimestamp  = 0x3fffffffffffff01ULL;
const TTimestamp AsyncLastCommittedTimestamp = 0x3fffffffffffff04ULL;
const TTimestamp UncommittedTimestamp        = 0x3fffffffffffff02ULL;
const TTimestamp AsyncAllCommittedTimestamp  = 0x3fffffffffffff03ULL;
const TTimestamp NotPreparedTimestamp        = 0x3fffffffffffffffULL; // must be > SyncLastCommittedTimestamp

// Masks.
const TTimestamp TimestampCounterMask     = 0x000000003fffffffULL;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
