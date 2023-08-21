#pragma once

#include "public.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TLockManager
    : public TRefCounted
{
public:
    TLockManager();
    ~TLockManager();

    void Lock(TTimestamp timestamp, TTransactionId transactionId, bool confirmed);
    void Unlock(TTimestamp commitTimestamp, TTransactionId transactionId);
    TLockManagerEpoch GetEpoch() const;

    // COMPAT(ifsmirnov): ETabletReign::FixBulkInsertAtomicityNone
    bool HasTransaction(TTransactionId transactionId) const;

    std::vector<TTransactionId> ExtractUnconfirmedTransactionIds();
    bool HasUnconfirmedTransactions() const;

    void Wait(TTimestamp timestamp, TLockManagerEpoch epoch);
    TError ValidateTransactionConflict(TTimestamp startTimestamp) const;

    void BuildOrchidYson(NYTree::TFluentMap fluent) const;

    void Persist(const TStreamPersistenceContext& context);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TLockManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
