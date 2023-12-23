#pragma once

#include "public.h"

#include <yt/yt/server/node/tablet_node/object_detail.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_detail.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/persistent_queue.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ring_queue.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

class TTransaction
    : public NTransactionSupervisor::TTransactionBase<NTabletNode::TObjectBase>
    , public TRefTracked<TTransaction>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, HasLease);
    DEFINE_BYVAL_RW_PROPERTY(TDuration, Timeout);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTransactionSignature, Signature, NTransactionClient::InitialTransactionSignature);

    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, StartTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, PrepareTimestamp, NTransactionClient::NullTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, CommitTimestamp, NTransactionClient::NullTimestamp);

    DEFINE_BYVAL_RW_PROPERTY(TString, User);

public:
    explicit TTransaction(TTransactionId id);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    TCallback<void(TSaveContext&)> AsyncSave();
    void AsyncLoad(TLoadContext& context);

    TFuture<void> GetFinished() const;
    void SetFinished();
    void ResetFinished();

    TTimestamp GetPersistentPrepareTimestamp() const;

    TInstant GetStartTime() const;

    NObjectClient::TCellTag GetCellTag() const;

private:
    TPromise<void> Finished_ = NewPromise<void>();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
