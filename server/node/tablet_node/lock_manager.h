#pragma once

#include "public.h"

#include <yt/core/ytree/fluent.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TLockManager
    : public TRefCounted
{
public:
    TLockManager();

    ~TLockManager();

    void Lock(TTimestamp timestamp, TTransactionId transactionId, bool confirmed);
    std::vector<TTransactionId> RemoveUnconfirmedTransactions();
    void Unlock(TTimestamp commitTimestamp, TTransactionId transactionId);
    TLockManagerEpoch GetEpoch() const;

    void Wait(TTimestamp timestamp, TLockManagerEpoch epoch);
    TError ValidateTransactionConflict(TTimestamp startTimestamp) const;

    void BuildOrchidYson(NYTree::TFluentMap fluent) const;

    void Persist(const TStreamPersistenceContext& context);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TLockManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
