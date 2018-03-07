#pragma once

#include <yt/ytlib/object_client/public.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TTransactionActionData;

class TReqStartTransaction;
class TRspStartTransaction;

class TReqRegisterTransactionActions;
class TRspRegisterTransactionActions;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionType,
    ((Master)          (0)) // Accepted by both masters and tablets
    ((Tablet)          (1)) // Accepted by tablets only
);

DEFINE_ENUM(EAtomicity,
    ((Full)            (0)) // 2PC enabled, Percolator mode :)
    ((None)            (1)) // 2PC disabled, HBase mode
);

DEFINE_ENUM(EDurability,
    ((Sync)            (0)) // Wait for Hydra commit result
    ((Async)           (1)) // Reply as soon as the request is enqueued to Hydra
);

//! Only applies to ordered tables.
DEFINE_ENUM(ECommitOrdering,
    ((Weak)            (0)) // Rows are appended to tablet in order of participant commits
    ((Strong)          (1)) // Rows are appended to tablet in order of timestamps
);

DEFINE_ENUM(EErrorCode,
    ((NoSuchTransaction)  (11000))
);

struct TTransactionActionData;
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
using TTimestamp = ui64;

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
//! Used by TSortedDynamicStore to mark values being written by transactions.
const TTimestamp UncommittedTimestamp         = 0x3fffffffffffff02ULL;
//! Used by TSortedDynamicStore in TLockDescriptor::PrepareTimestamp.
//! Must be larger than SyncLastCommittedTimestamp.
const TTimestamp NotPreparedTimestamp         = 0x3fffffffffffffffULL;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
