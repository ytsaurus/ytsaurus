#pragma once

#include "common.h"

#include "../misc/lease_manager.h"

#include "../chunk_holder/common.h"

namespace NYT {
namespace NTransaction {

////////////////////////////////////////////////////////////////////////////////

class TTransaction
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TTransaction> TPtr;
    typedef yvector<NChunkHolder::TChunkId> TChunks;

    TTransaction(const TTransactionId& id)
        : Id(id)
    { }

    TLeaseManager::TLease GetLease() const
    {
        return Lease;
    }

    void SetLease(TLeaseManager::TLease lease)
    {
        Lease = lease;
    }

    TTransactionId GetId() const
    {
        return Id;
    }

    TChunks& AddedChunks()
    {
        return AddedChunks_;
    }

private:
    TTransactionId Id;
    TLeaseManager::TLease Lease;
    TChunks AddedChunks_;

};

////////////////////////////////////////////////////////////////////////////////

//! Handles transaction notifications occurring in TTransactionManager.
struct ITransactionHandler
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<ITransactionHandler> TPtr;

    //! Called when a new transaction is started.
    virtual void OnTransactionStarted(TTransaction::TPtr transaction) = 0;
    
    //! Called during transaction commit.
    virtual void OnTransactionCommitted(TTransaction::TPtr transaction) = 0;

    //! Called during transaction abort.
    virtual void OnTransactionAborted(TTransaction::TPtr transaction) = 0;
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NTransaction
} // namespace NYT
