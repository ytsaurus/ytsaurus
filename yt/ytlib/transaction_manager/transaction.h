#pragma once

#include "common.h"

#include "../chunk_holder/common.h"
#include "../cypress/common.h"

namespace NYT {
namespace NTransaction {

////////////////////////////////////////////////////////////////////////////////

class TTransaction
{
public:
    TTransaction(const TTransaction& other)
        : Id(other.Id)
        , AddedChunkIds_(other.AddedChunkIds_)
    { }

    TTransaction(const TTransactionId& id)
        : Id(id)
    { }

    TAutoPtr<TTransaction> Clone() const
    {
        return new TTransaction(*this);
    }

    TTransactionId GetId() const
    {
        return Id;
    }

    yvector<TChunkId>& AddedChunkIds()
    {
        return AddedChunkIds_;
    }

    yvector<NCypress::TLockId>& LockIds()
    {
        return LockIds_;
    }

private:
    TTransactionId Id;
    yvector<TChunkId> AddedChunkIds_;
    yvector<NCypress::TLockId> LockIds_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
