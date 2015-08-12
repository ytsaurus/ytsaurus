#pragma once

#include <core/misc/public.h>
#include <core/misc/enum.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionType,
    ((Master)          (0)) // Accepted by both masters and tablets
    ((Tablet)          (1)) // Accepted by tablets only
);

DEFINE_ENUM(EAtomicity,
    ((Full)            (0)) // P2C enabled, Percolator mode :)
    ((None)            (1)) // P2C disabled, HBase mode
);

DEFINE_ENUM(EDurability,
    ((Sync)            (0)) // Wait for Hydra commit result
    ((Async)           (1)) // Reply as soon as the request is enqueued to Hydra
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
//! used to implement the MVCC paradigm.
/*!
 *  Timestamp is a 64-bit unsigned integer of the following structure:
 *  bits  0-29:  auto-incrementing counter (allowing up to ~10^9 timestamps per second)
 *  bits 30-61:  Unix time in seconds (from 1 Jan 1970)
 *  bits 62-63:  reserved
 */
typedef ui64 TTimestamp;

//! Number of bits in the counter part.
const int TimestampCounterWidth = 30;

// Timestamp values range:
//! Minimum valid (non-sentinel) timestamp.
const TTimestamp MinTimestamp                 = 0x0000000000000001ULL;
//! Maximum valid (non-sentinel) timestamp.
const TTimestamp MaxTimestamp                 = 0x3fffffffffffff00ULL;

// User sentinels:
//! Uninitialized/invalid timestamp.
const TTimestamp NullTimestamp                = 0x0000000000000000ULL;
//! Truly (serializable) latest committed version.
//! May cause row blocking if concurrent writes are in progress.
const TTimestamp SyncLastCommittedTimestamp   = 0x3fffffffffffff01ULL;
//! Relaxed (non-serializable) latest committed version.
//! Never leads to row blocking but may miss some concurrent writes.
const TTimestamp AsyncLastCommittedTimestamp  = 0x3fffffffffffff04ULL;
//! Used to fetch all committed values during e.g. flushes or compactions.
//! Returns all versions that were committed at the moment the reader was created.
//! Never leads to row blocking but may miss some concurrent writes.
const TTimestamp AllCommittedTimestamp        = 0x3fffffffffffff03ULL;

// System sentinels:
//! Used by TDynamicMemoryStore to mark values being written by transactions.
const TTimestamp UncommittedTimestamp         = 0x3fffffffffffff02ULL;
//! Used by TDynamicMemoryStore in TLockDescriptor::PrepareTimestamp.
//! TMust be larger than SyncLastCommittedTimestamp.
const TTimestamp NotPreparedTimestamp         = 0x3fffffffffffffffULL;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
