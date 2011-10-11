#pragma once

#include "common.h"
#include "../chunk_holder/common.h"

namespace NYT {
namespace NTransaction {

////////////////////////////////////////////////////////////////////////////////

class TTransaction
{
public:
    TTransaction(const TTransaction& other)
        : Id(other.Id)
        , AddedChunks_(other.AddedChunks_)
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

    yvector<TChunkId>& AddedChunks()
    {
        return AddedChunks_;
    }

private:
    TTransactionId Id;
    yvector<TChunkId> AddedChunks_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransaction
} // namespace NYT
