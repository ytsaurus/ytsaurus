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
    TTabletId TabletId;
    TSharedRef Data;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    i64 GetByteSize() const;
};

const size_t TransactionWriteLogChunkSize = 256;
using TTransactionWriteLog = TPersistentQueue<TTransactionWriteRecord, TransactionWriteLogChunkSize>;
using TTransactionWriteLogSnapshot = TPersistentQueueSnapshot<TTransactionWriteRecord, TransactionWriteLogChunkSize>;

////////////////////////////////////////////////////////////////////////////////

class TTransaction
    : public NHiveServer::TTransactionBase<TObjectBase>
    , public TRefTracked<TTransaction>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, Transient);
    DEFINE_BYVAL_RW_PROPERTY(bool, HasLease);
    DEFINE_BYVAL_RW_PROPERTY(TDuration, Timeout);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, StartTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, PrepareTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, CommitTimestamp);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TSortedDynamicRowRef>, LockedSortedRows);
    DEFINE_BYREF_RW_PROPERTY(TRingQueue<TSortedDynamicRowRef>, PrelockedSortedRows);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TOrderedDynamicRowRef>, LockedOrderedRows);
    DEFINE_BYREF_RW_PROPERTY(TRingQueue<TOrderedDynamicRowRef>, PrelockedOrderedRows);
    DEFINE_BYREF_RW_PROPERTY(TTransactionWriteLog, ImmediateWriteLog);
    DEFINE_BYREF_RW_PROPERTY(TTransactionWriteLog, DelayedWriteLog);
    DEFINE_BYVAL_RW_PROPERTY(TTransactionSignature, PersistentSignature);
    DEFINE_BYVAL_RW_PROPERTY(TTransactionSignature, TransientSignature);

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

private:
    TPromise<void> Finished_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
