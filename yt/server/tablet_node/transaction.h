#pragma once

#include "public.h"
#include "dynamic_memory_store_bits.h"

#include <core/misc/property.h>
#include <core/misc/ref_tracked.h>
#include <core/misc/ring_queue.h>

#include <core/actions/future.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTransaction
    : public TRefTracked<TTransaction>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TTransactionId, Id);
    DEFINE_BYVAL_RW_PROPERTY(TLease, Lease);
    DEFINE_BYVAL_RW_PROPERTY(TDuration, Timeout);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, StartTime);
    DEFINE_BYVAL_RW_PROPERTY(ETransactionState, State);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, StartTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, PrepareTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, CommitTimestamp);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TDynamicRowRef>, LockedRows);
    DEFINE_BYREF_RW_PROPERTY(TRingQueue<TDynamicRowRef>, PrelockedRows);

public:
    explicit TTransaction(const TTransactionId& id);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    TFuture<void> GetFinished() const;
    void SetFinished();
    void ResetFinished();

    ETransactionState GetPersistentState() const;
    TTimestamp GetPersistentPrepareTimestamp() const;

    void ThrowInvalidState() const;

private:
    TPromise<void> Finished_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
