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
    typedef yvector<TChunkId> TChunks;

    //! For serialization.
    TTransaction()
    { }

    TTransaction(const TTransactionId& id)
        : Id(id)
    { }

    TTransactionId GetId() const
    {
        return Id;
    }

    TLeaseManager::TLease& Lease()
    {
        return Lease_;
    }

    TChunks& AddedChunks()
    {
        return AddedChunks_;
    }

private:
    TTransactionId Id;
    TLeaseManager::TLease Lease_;
    TChunks AddedChunks_;

    friend TOutputStream& operator << (TOutputStream& stream, const TTransaction& transaction);
    friend TInputStream& operator >> (TInputStream& stream, TTransaction& transaction);

};

inline TOutputStream& operator << (TOutputStream& stream, const TTransaction& transaction)
{
    stream << transaction.Id
           << transaction.AddedChunks_;
    return stream;
}

inline TInputStream& operator >> (TInputStream& stream, TTransaction& transaction)
{
    stream >> transaction.Id
           >> transaction.AddedChunks_;
    return stream;
}

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
