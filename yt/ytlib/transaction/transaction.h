#pragma once

#include "common.h"

#include "../misc/lease_manager.h"

#include "../chunk_holder/common.h"

namespace NYT {
namespace NTransaction {

////////////////////////////////////////////////////////////////////////////////

struct TTransaction
{
    typedef yvector<TChunkId> TChunks;

    TTransaction()
    { }

    TTransaction(const TTransaction& other)
        : Id(other.Id)
        , Lease(other.Lease)
        , AddedChunks(other.AddedChunks)
    { }

    TTransaction(const TTransactionId& id)
        : Id(id)
    { }

    TTransaction& operator = (const TTransaction& other)
    {
        // TODO: implement
        UNUSED(other);
        UNUSED(other);
        YASSERT(false);
        return *this;
    }

    TTransactionId Id;
    TLeaseManager::TLease Lease;
    TChunks AddedChunks;
};

// TODO: move to cpp
inline TOutputStream& operator << (TOutputStream& stream, const TTransaction& transaction)
{
    stream << transaction.Id
           << transaction.AddedChunks;
    return stream;
}

inline TInputStream& operator >> (TInputStream& stream, TTransaction& transaction)
{
    stream >> transaction.Id
           >> transaction.AddedChunks;
    return stream;
}

////////////////////////////////////////////////////////////////////////////////

//! Handles transaction notifications occurring in TTransactionManager.
struct ITransactionHandler
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<ITransactionHandler> TPtr;

    //! Called when a new transaction is started.
    virtual void OnTransactionStarted(TTransaction& transaction) = 0;
    
    //! Called during transaction commit.
    virtual void OnTransactionCommitted(TTransaction& transaction) = 0;

    //! Called during transaction abort.
    virtual void OnTransactionAborted(TTransaction& transaction) = 0;
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NTransaction
} // namespace NYT
