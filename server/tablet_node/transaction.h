#pragma once

#include "public.h"
#include "object_detail.h"
#include "dynamic_store_bits.h"

#include <yt/server/hive/transaction_detail.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/persistent_queue.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/ring_queue.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionWriteRecord
{
    TTransactionWriteRecord() = default;
    TTransactionWriteRecord(
        const TTabletId& tabletId,
        TSharedRef data,
        int rowCount,
        size_t byteSize,
        const TSyncReplicaIdList& syncReplicaIds);

    TTabletId TabletId;
    TSharedRef Data;
    int RowCount = 0;
    size_t DataWeight = 0;
    TSyncReplicaIdList SyncReplicaIds;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    i64 GetByteSize() const;
};

constexpr size_t TransactionWriteLogChunkSize = 256;
using TTransactionWriteLog = TPersistentQueue<TTransactionWriteRecord, TransactionWriteLogChunkSize>;
using TTransactionWriteLogSnapshot = TPersistentQueueSnapshot<TTransactionWriteRecord, TransactionWriteLogChunkSize>;

////////////////////////////////////////////////////////////////////////////////

class TTransaction
    : public NHiveServer::TTransactionBase<TObjectBase>
    , public TRefTracked<TTransaction>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, Transient);
    DEFINE_BYVAL_RW_PROPERTY(bool, Foreign);
    DEFINE_BYVAL_RW_PROPERTY(bool, HasLease);
    DEFINE_BYVAL_RW_PROPERTY(TDuration, Timeout);

    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, StartTimestamp, NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, PrepareTimestamp, NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, CommitTimestamp, NullTimestamp);

    DEFINE_BYREF_RW_PROPERTY(TRingQueue<TSortedDynamicRowRef>, PrelockedRows);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TSortedDynamicRowRef>, LockedRows);

    DEFINE_BYREF_RW_PROPERTY(std::vector<TTablet*>, LockedTablets);

    DEFINE_BYREF_RW_PROPERTY(TTransactionWriteLog, ImmediateLockedWriteLog);
    DEFINE_BYREF_RW_PROPERTY(TTransactionWriteLog, ImmediateLocklessWriteLog);
    DEFINE_BYREF_RW_PROPERTY(TTransactionWriteLog, DelayedLocklessWriteLog);

    DEFINE_BYVAL_RW_PROPERTY(TTransactionSignature, PersistentSignature, InitialTransactionSignature);
    DEFINE_BYVAL_RW_PROPERTY(TTransactionSignature, TransientSignature, InitialTransactionSignature);

    DEFINE_BYVAL_RW_PROPERTY(bool, ReplicatedRowsPrepared, false);
    DEFINE_BYVAL_RW_PROPERTY(TString, User);

public:
    explicit TTransaction(const TTransactionId& id);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    TCallback<void(TSaveContext&)> AsyncSave();
    void AsyncLoad(TLoadContext& context);

    TFuture<void> GetFinished() const;
    void SetFinished();
    void ResetFinished();

    TTimestamp GetPersistentPrepareTimestamp() const;

    TInstant GetStartTime() const;

    bool IsAborted() const;
    bool IsActive() const;
    bool IsCommitted() const;
    bool IsPrepared() const;

    bool IsSerializationNeeded() const;

private:
    TPromise<void> Finished_ = NewPromise<void>();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
